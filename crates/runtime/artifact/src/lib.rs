//! Artifact application contracts and the atomic Tool/Artifact/Ledger commit.

#![forbid(unsafe_code)]

mod projected_application;
pub use projected_application::ProjectedArtifactApplication;
mod workspace;
pub use workspace::{GetWorkspaceFileArtifact, WorkspaceFileArtifactApplication};
mod file_representation;
mod file_tools;
mod native_file_capture;
pub use file_representation::{ArtifactFileRepresentation, artifact_file_representation};
pub use native_file_capture::{
    CONTENT_PROGRESS_STEP_BYTES, ContentProgress, NativeFileCapture, VerifyingSink,
};
mod media;
mod media_journal;
pub use media_journal::{MediaGenerationJournal, MediaJournalError, RecordedMediaResult};

mod proposal;
mod tools;

pub use file_tools::{BuiltinFileTools, FileToolPreparation, GeneratedFile};
pub use media::{
    BackendMediaAssetProof, BackendMediaCapabilityProvider, BackendMediaPhase,
    BackendMediaProviderOutcome, BackendMediaSourceProof, MediaMaterializationError,
    media_asset_content_id,
};
pub use proposal::{
    ApplyArtifactProposal, ArtifactProposalDisposition, ArtifactProposalReceipt,
    ArtifactProposalStore, CloseArtifactProposal, ProposeArtifactEdit,
};
pub use tools::{
    ArtifactOutputSchemaValidationError, ArtifactOutputSchemaValidator, ArtifactToolContext,
    ArtifactToolError, ArtifactToolPreparation, BackendMediaAsset, BackendMediaSource,
    BackendMediaSourceKind, BuiltinArtifactTools,
};

use musteragent_artifact::{Artifact, ArtifactKind, ArtifactOrigin, ArtifactVersion};
use musteragent_event_envelope::LedgerRecordKind;
use musteragent_identity::{
    Actor, ArtifactId, ConversationId, InvocationId, LedgerRecordId, TenantId, VersionNumber,
};
use musteragent_ledger::{AppendReceipt, AppendRequest};
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

pub const MAX_ARTIFACT_PAGE_SIZE: usize = 200;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ArtifactMutation {
    Create {
        artifact: Artifact,
        version: ArtifactVersion,
    },
    Update {
        artifact_id: ArtifactId,
        expected_version: VersionNumber,
        version: ArtifactVersion,
        title: Option<String>,
    },
}

impl ArtifactMutation {
    #[must_use]
    pub const fn artifact_id(&self) -> ArtifactId {
        match self {
            Self::Create { artifact, .. } => artifact.artifact_id,
            Self::Update { artifact_id, .. } => *artifact_id,
        }
    }

    #[must_use]
    pub const fn version(&self) -> &ArtifactVersion {
        match self {
            Self::Create { version, .. } | Self::Update { version, .. } => version,
        }
    }

    #[must_use]
    pub const fn conversation_id(&self) -> Option<ConversationId> {
        match self {
            Self::Create { artifact, .. } => artifact.source.conversation_id(),
            Self::Update { .. } => None,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ArtifactSemanticRecords {
    pub tool_succeeded_record_id: LedgerRecordId,
    pub artifact_created_record_id: Option<LedgerRecordId>,
    pub version_committed_record_id: LedgerRecordId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommitToolArtifact {
    pub tenant_id: TenantId,
    pub invocation_id: InvocationId,
    pub result: CapabilityResult,
    pub mutation: ArtifactMutation,
    pub append: AppendRequest,
    pub semantic_records: ArtifactSemanticRecords,
}

impl CommitToolArtifact {
    pub fn validate(&self) -> Result<(), ArtifactRuntimeError> {
        self.append
            .validate()
            .map_err(|_| ArtifactRuntimeError::InvalidLedgerAppend)?;
        if self.append.tenant_id != self.tenant_id {
            return Err(ArtifactRuntimeError::IdentityMismatch);
        }
        self.result
            .validate()
            .map_err(|_| ArtifactRuntimeError::ResultMismatch)?;
        let version = self.mutation.version();
        version
            .validate()
            .map_err(|_| ArtifactRuntimeError::VersionMismatch)?;
        if version.artifact_id != self.mutation.artifact_id()
            || version.source_invocation_id != Some(self.invocation_id)
        {
            return Err(ArtifactRuntimeError::VersionMismatch);
        }
        match &self.mutation {
            ArtifactMutation::Create { artifact, version } => {
                artifact
                    .validate()
                    .map_err(|_| ArtifactRuntimeError::InvalidArtifact)?;
                if artifact.tenant_id != self.tenant_id
                    || artifact.source.conversation_id() != Some(self.append.conversation_id)
                    || artifact.current_version.get() != 1
                    || version.version.get() != 1
                    || self.semantic_records.artifact_created_record_id.is_none()
                {
                    return Err(ArtifactRuntimeError::IdentityMismatch);
                }
            }
            ArtifactMutation::Update {
                expected_version,
                version,
                title,
                ..
            } => {
                let expected_next = expected_version
                    .get()
                    .checked_add(1)
                    .ok_or(ArtifactRuntimeError::VersionMismatch)?;
                if version.version.get() != expected_next
                    || self.semantic_records.artifact_created_record_id.is_some()
                {
                    return Err(ArtifactRuntimeError::VersionMismatch);
                }
                if title
                    .as_ref()
                    .is_some_and(|value| value.trim().is_empty() || value.chars().count() > 512)
                {
                    return Err(ArtifactRuntimeError::InvalidArtifact);
                }
            }
        }
        let record_ids = [
            Some(self.semantic_records.tool_succeeded_record_id),
            self.semantic_records.artifact_created_record_id,
            Some(self.semantic_records.version_committed_record_id),
        ];
        let expected_count = record_ids.into_iter().flatten().count();
        let matching_count = self
            .append
            .records
            .iter()
            .filter(|record| {
                record.record_kind == LedgerRecordKind::DomainEvent
                    && record_ids.contains(&Some(record.record_id))
            })
            .count();
        if matching_count != expected_count {
            return Err(ArtifactRuntimeError::MissingSemanticRecord);
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ArtifactCommitDisposition {
    Committed,
    Duplicate,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactCommitReceipt {
    pub disposition: ArtifactCommitDisposition,
    pub artifact_id: ArtifactId,
    pub version: VersionNumber,
    pub ledger: AppendReceipt,
}

/// High-level prepared Artifact mutation submitted by a live Worker.
///
/// The infrastructure committer derives all Ledger records, sequences and
/// idempotency keys itself and fences the aggregate + Tool terminal transition
/// in one database transaction.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommitPreparedArtifactTool {
    pub resource_replacement: Option<musteragent_runtime_resource::PreparedResourceFileReplacement>,
    pub run_context: RunContext,
    pub invocation_id: InvocationId,
    pub result: CapabilityResult,
    pub authorized_content: Vec<musteragent_content_ref::ContentRef>,
    pub mutation: ArtifactMutation,
    pub resource_placement: Option<musteragent_runtime_resource::PreparedResourceFilePlacement>,
    pub committed_at: musteragent_identity::UtcTimestamp,
}

impl CommitPreparedArtifactTool {
    pub fn validate(&self, attempt: &AttemptContext) -> Result<(), ArtifactRuntimeError> {
        self.result
            .validate()
            .map_err(|_| ArtifactRuntimeError::ResultMismatch)?;
        let version = self.mutation.version();
        version
            .validate()
            .map_err(|_| ArtifactRuntimeError::VersionMismatch)?;
        if attempt.run_id != self.run_context.run_id
            || version.artifact_id != self.mutation.artifact_id()
            || version.source_invocation_id != Some(self.invocation_id)
            || version.committed_at != self.committed_at
        {
            return Err(ArtifactRuntimeError::IdentityMismatch);
        }
        // A newly created File may return its own content for a later Tool in
        // this Run. Other references must belong to its declared dependencies.
        let creates_file = matches!(
            &self.mutation,
            ArtifactMutation::Create { artifact, .. } if artifact.kind == ArtifactKind::File
        );
        if self.authorized_content.iter().any(|reference| {
            !(version.dependency_refs.contains(reference)
                || creates_file && reference == &version.content_ref)
        }) {
            return Err(ArtifactRuntimeError::ResultMismatch);
        }
        if let ArtifactMutation::Create { artifact, .. } = &self.mutation
            && (artifact.tenant_id != self.run_context.tenant_id
                || artifact.source.conversation_id() != Some(self.run_context.conversation_id)
                || artifact.origin
                    != (ArtifactOrigin::Runtime {
                        activity_id: self.run_context.activity_id,
                        created_by_run_id: self.run_context.run_id,
                    }))
        {
            return Err(ArtifactRuntimeError::IdentityMismatch);
        }
        let created_file_id = match &self.mutation {
            ArtifactMutation::Create { artifact, .. } if artifact.kind == ArtifactKind::File => {
                Some(artifact.artifact_id)
            }
            _ => None,
        };
        if created_file_id.is_some() != self.resource_placement.is_some() {
            return Err(ArtifactRuntimeError::InvalidResourcePlacement);
        }
        if let Some(placement) = &self.resource_placement {
            placement
                .validate()
                .map_err(|_| ArtifactRuntimeError::InvalidResourcePlacement)?;
            let is_file_create = matches!(
                &self.mutation,
                ArtifactMutation::Create { artifact, .. }
                    if artifact.kind == ArtifactKind::File
            );
            if !is_file_create
                || placement.context.tenant_id != self.run_context.tenant_id
                || placement.context.conversation_id() != Some(self.run_context.conversation_id)
                || placement.context.invocation_id != Some(self.invocation_id)
                || placement.node_id.into_uuid()
                    != created_file_id
                        .expect("placement presence proves File create")
                        .into_uuid()
            {
                return Err(ArtifactRuntimeError::InvalidResourcePlacement);
            }
        }
        if let Some(target) = &self.resource_replacement {
            target
                .context
                .validate()
                .map_err(|_| ArtifactRuntimeError::IdentityMismatch)?;
            if target.context.tenant_id != self.run_context.tenant_id
                || target.context.conversation_id() != Some(self.run_context.conversation_id)
                || target.context.invocation_id != Some(self.invocation_id)
                || target.context.now != self.committed_at
                || self.resource_placement.is_some()
                || !matches!(&self.mutation, ArtifactMutation::Update { artifact_id, expected_version, version, title }
                    if *artifact_id == target.file.artifact_id
                        && *expected_version == target.file.version
                        && Some(version.version.get()) == expected_version.get().checked_add(1)
                        && version.content_ref.media_type == target.file.content_ref.media_type
                        && title.is_none())
            {
                return Err(ArtifactRuntimeError::IdentityMismatch);
            }
        }
        Ok(())
    }
}

/// Stable execution semantics for persistence-specific Artifact commit errors.
///
/// Committers retain their diagnostic-rich native error types, while Runtime
/// handlers consume this closed classification instead of guessing that every
/// storage error is a retryable outage.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ArtifactToolCommitFailure {
    WriterBusy,
    StaleFence,
    InvalidCommand,
    Authorization,
    NameConflict,
    Conflict,
    Unavailable,
    Corrupt,
}

pub trait ClassifyArtifactToolCommitError {
    fn failure(&self) -> ArtifactToolCommitFailure;
}

/// Production-only atomic committer. Specialized Artifact handlers must use
/// this port instead of composing a generic Tool completion with a later
/// Artifact write. Every adapter must preserve its native diagnostics while
/// exposing the same closed failure semantics to all Runtime handlers.
pub trait FencedArtifactToolCommitter: Send + Sync {
    type Error: ClassifyArtifactToolCommitError + Send;

    fn commit(
        &self,
        attempt: &AttemptContext,
        command: CommitPreparedArtifactTool,
    ) -> ApplicationFuture<'_, Result<ArtifactCommitReceipt, Self::Error>>;
    /// Read the original Tool + result + Ledger terminal in its existing owner.
    /// Implementations authenticate the current Attempt, exact original request
    /// and prebound identity, honor the finite deadline, and never execute work.
    /// Missing or nonterminal state is unknown, not proof of no effect.
    fn lookup_original_receipt(
        &self,
        _attempt: &AttemptContext,
        _query: musteragent_runtime_capability::OriginalToolReceiptQuery,
    ) -> ApplicationFuture<
        '_,
        Result<
            Option<musteragent_runtime_capability::ToolTerminalOutcome>,
            musteragent_runtime_capability::ToolExecutionPortError,
        >,
    > {
        Box::pin(async {
            Err(musteragent_runtime_capability::ToolExecutionPortError {
                code: musteragent_runtime_contract::ErrorCode::CapabilityUnavailable,
                retryable: false,
            })
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactView {
    pub artifact: Artifact,
    pub versions: Vec<ArtifactVersion>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactSummary {
    pub artifact: Artifact,
    pub current_version: ArtifactVersion,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct GetArtifact {
    pub actor: Actor,
    pub artifact_id: ArtifactId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ListConversationArtifacts {
    pub actor: Actor,
    pub conversation_id: ConversationId,
    pub limit: usize,
    pub before_artifact_id: Option<ArtifactId>,
    pub search_query: Option<String>,
    pub kinds: Vec<ArtifactKind>,
}

impl ListConversationArtifacts {
    pub fn validate(&self) -> Result<(), ArtifactRuntimeError> {
        if self.limit == 0
            || self.limit > MAX_ARTIFACT_PAGE_SIZE
            || self
                .search_query
                .as_ref()
                .is_some_and(|value| value.trim().is_empty() || value.chars().count() > 200)
            || self.kinds.len() > 6
        {
            Err(ArtifactRuntimeError::InvalidPageSize)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactPage {
    pub artifacts: Vec<ArtifactSummary>,
    pub next_before_artifact_id: Option<ArtifactId>,
    pub has_more: bool,
}

/// Authenticated public Artifact queries. Implementations must scope every
/// query to both the tenant and an active Conversation participant.
pub trait ArtifactApplication: Send + Sync {
    fn get_artifact(
        &self,
        query: GetArtifact,
    ) -> ApplicationFuture<'_, Result<ArtifactView, musteragent_runtime_contract::RuntimeError>>;

    fn list_conversation_artifacts(
        &self,
        query: ListConversationArtifacts,
    ) -> ApplicationFuture<'_, Result<ArtifactPage, musteragent_runtime_contract::RuntimeError>>;
}

/// Conversation-scoped Artifact read authority required by Runtime-owned Tool handlers.
///
/// Keeping this port separate from [`ArtifactStore`] lets each execution home
/// pair the shared Tool semantics with its own fenced atomic committer. A
/// Device reader therefore never has to provide a second, unused mutation
/// path merely to reuse the canonical Artifact tools. Tool handlers must use
/// this port instead of the tenant-wide internal [`ArtifactStore::load`]; an
/// Artifact UUID is an identity, never a bearer credential.
pub trait ArtifactReader: Send + Sync {
    type Error: Send;

    fn load_for_conversation<'a>(
        &'a self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        artifact_id: ArtifactId,
    ) -> ApplicationFuture<'a, Result<Option<ArtifactView>, Self::Error>>;

    fn list(
        &self,
        query: ListArtifacts,
    ) -> ApplicationFuture<'_, Result<Vec<Artifact>, Self::Error>>;
}

/// Stable semantic classification for failures returned by an
/// [`ArtifactReader`]. Runtime Tool handlers must not collapse a temporary
/// storage outage into a permanent "not found" response.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ArtifactReadFailure {
    NotFound,
    Unavailable,
    Corrupt,
}

pub trait ClassifyArtifactReadError {
    fn artifact_read_failure(&self) -> ArtifactReadFailure;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ListArtifacts {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub limit: usize,
    pub before_artifact_id: Option<ArtifactId>,
}

impl ListArtifacts {
    pub fn validate(&self) -> Result<(), ArtifactRuntimeError> {
        if self.limit == 0 || self.limit > MAX_ARTIFACT_PAGE_SIZE {
            Err(ArtifactRuntimeError::InvalidPageSize)
        } else {
            Ok(())
        }
    }
}

/// Store implementations must commit the Tool terminal transition, Artifact
/// aggregate/version, semantic Ledger records and Outbox in one transaction.
pub trait ArtifactStore: Send + Sync {
    type Error: Send;

    fn commit_tool_artifact(
        &self,
        command: CommitToolArtifact,
    ) -> ApplicationFuture<'_, Result<ArtifactCommitReceipt, Self::Error>>;

    fn load<'a>(
        &'a self,
        tenant_id: TenantId,
        artifact_id: ArtifactId,
    ) -> ApplicationFuture<'a, Result<ArtifactView, Self::Error>>;

    fn list(
        &self,
        query: ListArtifacts,
    ) -> ApplicationFuture<'_, Result<Vec<Artifact>, Self::Error>>;
}

impl<T> ArtifactReader for T
where
    T: ArtifactStore,
{
    type Error = T::Error;

    fn load_for_conversation<'a>(
        &'a self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        artifact_id: ArtifactId,
    ) -> ApplicationFuture<'a, Result<Option<ArtifactView>, Self::Error>> {
        Box::pin(async move {
            let view = ArtifactStore::load(self, tenant_id, artifact_id).await?;
            Ok((view.artifact.source.conversation_id() == Some(conversation_id)).then_some(view))
        })
    }

    fn list(
        &self,
        query: ListArtifacts,
    ) -> ApplicationFuture<'_, Result<Vec<Artifact>, Self::Error>> {
        ArtifactStore::list(self, query)
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ArtifactRuntimeError {
    #[error("Artifact commit identity does not match its tenant or Conversation")]
    IdentityMismatch,
    #[error("Artifact aggregate is invalid")]
    InvalidArtifact,
    #[error("Artifact version is inconsistent with the requested mutation")]
    VersionMismatch,
    #[error("Tool result does not reference the committed Artifact content")]
    ResultMismatch,
    #[error("Artifact commit Ledger append is invalid")]
    InvalidLedgerAppend,
    #[error("Artifact commit is missing its typed semantic Ledger record")]
    MissingSemanticRecord,
    #[error("Artifact list page size must be within 1..=200")]
    InvalidPageSize,
    #[error("Artifact Proposal violates its state, identity or resolution contract")]
    InvalidProposal,
    #[error("File Artifact resource-tree placement is invalid")]
    InvalidResourcePlacement,
}

#[cfg(test)]
mod tests {
    use musteragent_artifact::{
        Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactSource, ArtifactVersion,
        ArtifactVersionAuthor,
    };
    use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
    use musteragent_event_envelope::{LedgerRecordKind, Producer};
    use musteragent_identity::{
        ActivityId, ArtifactId, ArtifactVersionId, ContentId, ConversationId, CorrelationId,
        DottedKey, HomeEpoch, IdempotencyKey, InvocationId, LedgerOffset, LedgerRecordId,
        ParticipantId, RunId, TenantId, UtcTimestamp, VersionNumber, VersionedKey,
    };
    use musteragent_ledger::{AppendRequest, ProposedRecord};
    use musteragent_runtime_capability::CapabilityResult;

    use super::{
        ArtifactMutation, ArtifactRuntimeError, ArtifactSemanticRecords, CommitToolArtifact,
    };

    #[test]
    fn result_and_semantic_records_are_part_of_the_atomic_contract() {
        let tenant_id = TenantId::generate();
        let conversation_id = ConversationId::generate();
        let invocation_id = InvocationId::generate();
        let artifact_id = ArtifactId::generate();
        let content = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").expect("media"),
            byte_length: 2,
            sha256: Sha256::digest(b"{}"),
        };
        let tool_record_id = LedgerRecordId::generate();
        let created_record_id = LedgerRecordId::generate();
        let version_record_id = LedgerRecordId::generate();
        let records = [tool_record_id, created_record_id, version_record_id]
            .into_iter()
            .map(|record_id| ProposedRecord {
                schema_version: DottedKey::new("event.v1").expect("schema"),
                record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: None,
                run_id: None,
                run_seq: None,
                attempt_id: None,
                participant_id: None,
                producer: Producer::Worker,
                occurred_at: UtcTimestamp::from_unix_millis(1),
                causation_id: None,
                correlation_id: CorrelationId::generate(),
                payload: vec![1],
                payload_hash: Sha256::digest([1]),
            })
            .collect();
        let command = CommitToolArtifact {
            tenant_id,
            invocation_id,
            result: CapabilityResult {
                result_ref: content.clone(),
                result_hash: content.sha256,
                result_schema: SchemaRef {
                    schema_key: VersionedKey::new("artifact.result@1").expect("schema"),
                    schema_hash: Sha256::digest(b"schema"),
                },
                provider_request_ref: None,
            },
            mutation: ArtifactMutation::Create {
                artifact: Artifact {
                    artifact_id,
                    tenant_id,
                    source: ArtifactSource::Conversation(conversation_id),
                    origin: ArtifactOrigin::Runtime {
                        activity_id: ActivityId::generate(),
                        created_by_run_id: RunId::generate(),
                    },
                    kind: ArtifactKind::Note,
                    lifecycle: ArtifactLifecycle::Draft,
                    current_version: VersionNumber::new(1).expect("version"),
                    title: "Note".into(),
                    created_at: UtcTimestamp::from_unix_millis(1),
                    updated_at: UtcTimestamp::from_unix_millis(1),
                },
                version: ArtifactVersion {
                    artifact_version_id: ArtifactVersionId::generate(),
                    artifact_id,
                    version: VersionNumber::new(1).expect("version"),
                    content_ref: content,
                    dependency_refs: Vec::new(),
                    content_hash: Sha256::digest(b"{}"),
                    source_invocation_id: Some(invocation_id),
                    committed_by: ArtifactVersionAuthor::Participant(ParticipantId::generate()),
                    committed_at: UtcTimestamp::from_unix_millis(1),
                },
            },
            append: AppendRequest {
                tenant_id,
                conversation_id,
                expected_home_epoch: HomeEpoch::new(1).expect("epoch"),
                expected_offset: LedgerOffset::ZERO,
                idempotency_key: IdempotencyKey::new("artifact:commit:1").expect("key"),
                command_fingerprint: Sha256::digest(b"commit"),
                records,
                outbox: vec![],
            },
            semantic_records: ArtifactSemanticRecords {
                tool_succeeded_record_id: tool_record_id,
                artifact_created_record_id: Some(created_record_id),
                version_committed_record_id: version_record_id,
            },
        };
        assert_eq!(command.validate(), Ok(()));

        let mut invalid = command;
        invalid.semantic_records.version_committed_record_id = LedgerRecordId::generate();
        assert_eq!(
            invalid.validate(),
            Err(ArtifactRuntimeError::MissingSemanticRecord)
        );
    }
}
