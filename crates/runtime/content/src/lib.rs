//! Tenant-authorized, content-addressed byte storage port.
//!
//! Content metadata lives in the Runtime database while encrypted blobs may
//! live in local storage or cloud object storage. Consumers never receive a
//! storage locator and must validate immutable length/hash on every read.

#![forbid(unsafe_code)]

mod admission;
mod invocation;
pub use invocation::{
    InvocationContentLimits, InvocationContentStore, MAX_INVOCATION_CONTENT_OBJECTS,
};
mod batch;
pub mod model_reference;
pub use batch::ContentBatch;
mod read_set;
pub use read_set::RecordingContentStore;
mod range;
pub use range::{ContentRangeSource, VerifiedContentReader};
mod stream;
mod text;
pub use text::{ContentTextError, read_verified_text_page};
pub mod upload;
mod verified_cache;
pub use stream::{CONTENT_TRANSFER_CHUNK_BYTES, ContentSink, ContentSource, transfer_content};

pub use admission::{
    AdmissionContentPlan, AdmissionContentSlot, AdmissionContentSource, AdmissionContentStage,
    semantic_identity,
};
pub use verified_cache::{
    ContentCacheError, ContentCacheGeneration, ContentCacheRead, ContentCacheStats,
    RetainedContentMaterial, VerifiedContentCache,
};

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use std::fmt::{Debug, Formatter};

use musteragent_identity::{
    Actor, ArtifactId, ArtifactVersionId, ContentId, ConversationId, TenantId,
};
pub use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_contract::RuntimeError;
use thiserror::Error;

/// Largest single immutable content object.
///
/// Derived, not chosen: the resumable upload protocol carries at most
/// `MAX_CONTENT_UPLOAD_PARTS` (10,000 — the S3/R2 multipart part bound it
/// mirrors, which also keeps one completion request near 1.2 MB) parts of
/// `CONTENT_UPLOAD_PART_BYTES` (8 MiB) each, i.e. 78.125 GiB. Every layer below
/// the upload streams with bounded buffers, and object storage itself accepts
/// up to ~5 TiB, so this is the binding external limit. Paths that hold a
/// whole object in memory use `MAX_BUFFERED_CONTENT_BYTES` instead.
pub const MAX_CONTENT_BYTES: u64 =
    upload::MAX_CONTENT_UPLOAD_PARTS as u64 * upload::CONTENT_UPLOAD_PART_BYTES as u64;

/// Largest object an operation may hold as one in-memory `Vec<u8>`
/// (`PutContent`, `ContentStore::read`, buffered blob IO). Larger content is
/// only reachable through the streaming contracts (`put_stream_at`,
/// `copy_to`, `open_verified`), which never materialize the whole object.
pub const MAX_BUFFERED_CONTENT_BYTES: u64 = 264 * 1024 * 1024 + 12;
pub const MAX_STORAGE_LOCATOR_BYTES: usize = 2_048;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PutContent {
    pub tenant_id: TenantId,
    pub media_type: MediaType,
    pub bytes: Vec<u8>,
}

impl PutContent {
    pub fn validate(&self) -> Result<(), ContentStoreError> {
        if self.bytes.len() as u64 > MAX_BUFFERED_CONTENT_BYTES {
            return Err(ContentStoreError::TooLarge);
        }
        Ok(())
    }
}

/// Stable identity used to derive a tenant-separated object key. A staged
/// upload can therefore be reconciled after a process crash without persisting
/// a plaintext bucket/key locator.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct BlobIdentity {
    pub tenant_id: TenantId,
    pub content_id: ContentId,
}

/// Internal storage address. It is intentionally absent from every public
/// ContentRef and redacted from diagnostics.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct StorageLocator(String);

impl StorageLocator {
    pub fn new(value: impl Into<String>) -> Result<Self, BlobStoreError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_STORAGE_LOCATOR_BYTES
            || !value.is_ascii()
            || value.bytes().any(|value| value.is_ascii_control())
        {
            return Err(BlobStoreError::InvalidLocator);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Debug for StorageLocator {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("StorageLocator([REDACTED])")
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PutBlob {
    pub identity: BlobIdentity,
    pub sha256: Sha256,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BlobPutDisposition {
    Created,
    AlreadyPresent,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PutBlobReceipt {
    pub locator: StorageLocator,
    pub disposition: BlobPutDisposition,
}

/// Raw-byte persistence port. Implementations must use an atomic create and a
/// deterministic key for `BlobIdentity`; `AlreadyPresent` is never permission
/// to overwrite an existing object. Streaming is required at every adapter
/// layer; a missing implementation must fail compilation, not masquerade as
/// a retryable storage outage in a live invocation.
pub trait BlobStore: Send + Sync {
    /// Immutable bounded upload. The adapter validates all input and stored bytes,
    /// atomically creates the target, and never replaces an existing object.
    fn put_stream<'a>(
        &'a self,
        identity: BlobIdentity,
        length: u64,
        hash: Sha256,
        source: &'a mut dyn ContentSource,
    ) -> ApplicationFuture<'a, Result<PutBlobReceipt, ContentStoreError>>;
    /// Copy into private staging, validating exact length/hash before success.
    fn copy_to<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
        length: u64,
        hash: Sha256,
        sink: &'a mut dyn ContentSink,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>>;

    fn put(
        &self,
        request: PutBlob,
    ) -> ApplicationFuture<'_, Result<PutBlobReceipt, BlobStoreError>>;

    fn read<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, BlobStoreError>>;

    fn delete<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> ApplicationFuture<'a, Result<(), BlobStoreError>>;

    fn derive_locator(&self, identity: BlobIdentity) -> Result<StorageLocator, BlobStoreError>;

    /// Validates that a persisted locator belongs to this store and identity.
    ///
    /// The default contract accepts only the locator produced for new writes.
    /// Stores that retain historical namespace prefixes during a governed
    /// migration override this method without changing the current write key.
    fn validate_locator(
        &self,
        identity: BlobIdentity,
        locator: &StorageLocator,
    ) -> Result<(), BlobStoreError> {
        if self.derive_locator(identity)? == *locator {
            Ok(())
        } else {
            Err(BlobStoreError::LocatorMismatch)
        }
    }
}

pub trait ContentStore: Send + Sync {
    /// Metadata-only current tenant/full-reference/tombstone qualification for
    /// an already verified private snapshot. This grants no file association
    /// or Actor access. Callers reauthorize that original owner before/after IO.
    fn validate_current<'a>(
        &'a self,
        _tenant: TenantId,
        _reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }

    /// Internal producer recovery only. Look up an immutable prewrite at a
    /// caller-derived identity in its original tenant, without granting any
    /// execution or public read authority. Never expose this as model/API
    /// lookup. A tombstone or incomplete/corrupt object is an error, not
    /// permission to overwrite it. Unsupported adapters fail closed.
    fn find_prepared_at(
        &self,
        _tenant: TenantId,
        _content_id: ContentId,
    ) -> ApplicationFuture<'_, Result<Option<ContentRef>, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }
    /// Retains authorized material for bounded native random access. The
    /// complete bytes and fresh metadata are checked before returning; every
    /// later range must match that verified snapshot or fail. Material remains
    /// private and the original owner rechecks authority before publication.
    /// Unsupported stores fail without falling back to a whole-file Vec.
    fn open_verified<'a>(
        &'a self,
        _tenant: TenantId,
        _reference: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<VerifiedContentReader, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }

    /// Exact, immutable create-at using bounded sequential input. The store
    /// verifies source and final stored bytes before returning. Unsupported
    /// stores fail before consuming input; they must not buffer the whole file
    /// through `put_at`. This does not publish an Artifact or grant access.
    fn put_stream_at<'a>(
        &'a self,
        _tenant: TenantId,
        _reference: &'a ContentRef,
        _source: &'a mut dyn ContentSource,
    ) -> ApplicationFuture<'a, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }

    /// Copies exact authorized bytes into caller-owned PRIVATE staging. The
    /// sink is usable only after success; errors/cancellation require discard.
    /// Final success includes complete length/hash and fresh authority checks.
    /// Unsupported stores fail without reading or writing any payload bytes.
    fn copy_to<'a>(
        &'a self,
        _tenant: TenantId,
        _reference: &'a ContentRef,
        _sink: &'a mut dyn ContentSink,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }

    /// Optional process-local material reuse, sharing this Store's total byte
    /// budget and invalidation lifetime. Capture its generation BEFORE current
    /// source metadata is loaded; this handle grants no content authority.
    fn material_cache(&self) -> Option<std::sync::Arc<VerifiedContentCache>> {
        None
    }
    fn put(
        &self,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>>;

    /// Stores immutable material owned by the signed Runtime Release rather
    /// than by one tenant. Device stores may keep the bytes in the sole local
    /// tenant partition; multi-tenant Cloud stores override this operation to
    /// create one globally readable, non-deletable content object.
    ///
    /// This is an internal composition authority, not a public upload API.
    fn put_runtime_release(
        &self,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        self.put(request)
    }

    /// Stores immutable Runtime Release material at its published identity.
    ///
    /// Compiled release catalogs embed complete `ContentRef` values.  Their
    /// material therefore has to be reconciled at the declared ID rather than
    /// at an implementation-generated ID, otherwise a valid Snapshot can
    /// contain a permanently dangling reference.  Cloud stores override this
    /// operation to keep the object globally readable and non-deletable.
    fn put_runtime_release_at(
        &self,
        content_id: ContentId,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        self.put_at(content_id, request)
    }

    fn read<'a>(
        &'a self,
        tenant_id: TenantId,
        content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<Vec<u8>, ContentStoreError>>;

    /// Reads an ordered set of immutable ContentRefs under one tenant.
    ///
    /// The default implementation preserves compatibility for device stores
    /// and test doubles. Cloud stores should override this operation so one
    /// Product hydration batch does not degenerate into per-reference metadata
    /// and blob database round trips.
    fn read_batch<'a>(
        &'a self,
        tenant_id: TenantId,
        contents: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<Vec<Vec<u8>>, ContentStoreError>> {
        Box::pin(async move {
            let mut payloads = Vec::with_capacity(contents.len());
            for content in contents {
                payloads.push(self.read(tenant_id, content).await?);
            }
            Ok(payloads)
        })
    }

    /// Crash-reconcilable write at a caller-predeclared Content identity.
    /// Implementations that cannot guarantee immutable create-at semantics
    /// must fail closed before accepting recovery-sensitive payloads.
    fn put_at(
        &self,
        _content_id: ContentId,
        _request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }

    /// Stores a caller-predeclared immutable prewrite that may be reclaimed
    /// after a conservative grace period if it never becomes reachable from a
    /// canonical Runtime relation. Ordinary content must not use this path.
    fn put_reclaimable_at(
        &self,
        content_id: ContentId,
        request: PutContent,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContentStoreError>> {
        self.put_at(content_id, request)
    }

    /// Idempotently tombstones metadata and destroys the underlying blob.
    fn delete<'a>(
        &'a self,
        _tenant_id: TenantId,
        _content: &'a ContentRef,
    ) -> ApplicationFuture<'a, Result<(), ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }

    /// Creates or verifies an invisible, command-bound admission stage. The
    /// returned content is not product-readable until the owning application
    /// store publishes this receipt in its product transaction.
    fn prepare_admission(
        &self,
        _plan: AdmissionContentPlan,
    ) -> ApplicationFuture<'_, Result<AdmissionContentStage, ContentStoreError>> {
        Box::pin(async { Err(ContentStoreError::Unavailable) })
    }
}

/// Runtime-owned proof that one exact immutable ContentRef is reachable from
/// a current canonical Artifact in the authenticated Conversation.
///
/// Implementations must match tenant, Conversation and the complete immutable
/// identity (id, media type, length and hash). This is intentionally a narrow
/// per-reference authorization port; callers must never use it to enumerate or
/// inject all Conversation content into a Run.
pub trait ArtifactContentAuthorization: Send + Sync {
    fn is_current_content_reachable(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        reference: &ContentRef,
    ) -> ApplicationFuture<'_, Result<bool, RuntimeError>>;

    /// Resolves current Artifact reachability for an ordered ContentRef batch.
    ///
    /// The result is positionally aligned with `references`. Infrastructure
    /// implementations should override this operation with one set query;
    /// retaining the scalar default keeps non-cloud stores source compatible.
    fn current_content_reachability<'a>(
        &'a self,
        actor: Actor,
        conversation_id: ConversationId,
        references: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<Vec<bool>, RuntimeError>> {
        Box::pin(async move {
            let mut reachable = Vec::with_capacity(references.len());
            for reference in references {
                reachable.push(
                    self.is_current_content_reachable(actor, conversation_id, reference)
                        .await?,
                );
            }
            Ok(reachable)
        })
    }

    /// Proves that every requested ContentRef belongs to one exact immutable
    /// Artifact version visible to the actor in the authenticated Conversation.
    ///
    /// This authority is intentionally version-scoped: historical content must
    /// not be added to the moving Conversation manifest or made reachable by
    /// tenant identity alone. Implementations must accept only the version's
    /// primary ContentRef or one of its governed dependency ContentRefs, with
    /// the complete immutable identity matching exactly.
    fn version_content_reachability<'a>(
        &'a self,
        _actor: Actor,
        _conversation_id: ConversationId,
        _artifact_id: ArtifactId,
        _artifact_version_id: ArtifactVersionId,
        references: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<Vec<bool>, RuntimeError>> {
        Box::pin(async move { Ok(vec![false; references.len()]) })
    }

    /// Workspace File content is authorized by current active placement and an
    /// exact immutable version. The caller must independently authenticate a
    /// Workspace-scoped content grant; a Conversation ticket is not sufficient.
    /// Stores without native Workspace authority deny every reference.
    fn workspace_version_content_reachability<'a>(
        &'a self,
        _actor: Actor,
        _workspace_id: musteragent_identity::WorkspaceId,
        _artifact_id: ArtifactId,
        _artifact_version_id: ArtifactVersionId,
        references: &'a [ContentRef],
    ) -> ApplicationFuture<'a, Result<Vec<bool>, RuntimeError>> {
        Box::pin(async move { Ok(vec![false; references.len()]) })
    }

    /// Resolves one request-declared ContentId to its complete immutable
    /// ContentRef only when a current canonical Artifact makes it reachable.
    fn resolve_current_content(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        content_id: ContentId,
    ) -> ApplicationFuture<'_, Result<Option<ContentRef>, RuntimeError>>;
}

pub fn verify_content(reference: &ContentRef, bytes: &[u8]) -> Result<(), ContentStoreError> {
    if bytes.len() as u64 != reference.byte_length {
        return Err(ContentStoreError::LengthMismatch);
    }
    if Sha256::digest(bytes) != reference.sha256 {
        return Err(ContentStoreError::HashMismatch);
    }
    Ok(())
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum BlobStoreError {
    #[error("blob storage locator is invalid")]
    InvalidLocator,
    #[error("blob storage locator does not belong to the requested content")]
    LocatorMismatch,
    #[error("blob was not found")]
    NotFound,
    #[error("blob already exists with different immutable bytes")]
    IdentityConflict,
    #[error("blob exceeds the configured size limit")]
    TooLarge,
    #[error("blob storage dependency is unavailable")]
    Unavailable,
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ContentStoreError {
    #[error("content was not found")]
    NotFound,
    #[error("content is not authorized for this tenant")]
    Unauthorized,
    #[error("content exceeds the configured size limit")]
    TooLarge,
    #[error("content length differs from its immutable reference")]
    LengthMismatch,
    #[error("content hash differs from its immutable reference")]
    HashMismatch,
    #[error("content identity already exists with different immutable bytes")]
    IdentityConflict,
    #[error("content storage dependency is unavailable")]
    Unavailable,
    #[error("content storage invariant was violated")]
    Invariant,
}

impl From<BlobStoreError> for ContentStoreError {
    fn from(error: BlobStoreError) -> Self {
        match error {
            BlobStoreError::InvalidLocator => Self::Invariant,
            BlobStoreError::LocatorMismatch => Self::Unauthorized,
            BlobStoreError::NotFound => Self::NotFound,
            BlobStoreError::IdentityConflict => Self::IdentityConflict,
            BlobStoreError::TooLarge => Self::TooLarge,
            BlobStoreError::Unavailable => Self::Unavailable,
        }
    }
}

impl ContentStoreError {
    /// Retain the content owner's failure classification across orchestration
    /// adapters. A missing or corrupt object cannot heal by retrying its read.
    #[must_use]
    pub const fn runtime_code(self) -> musteragent_runtime_contract::ErrorCode {
        use musteragent_runtime_contract::ErrorCode;
        match self {
            Self::Unavailable => ErrorCode::ContentUnavailable,
            Self::NotFound => ErrorCode::ResourceFileUnavailable,
            Self::Unauthorized => ErrorCode::PolicyDenied,
            Self::TooLarge => ErrorCode::ModelContextExceeded,
            Self::LengthMismatch | Self::HashMismatch | Self::IdentityConflict => {
                ErrorCode::ContentIntegrityFailed
            }
            Self::Invariant => ErrorCode::InternalInvariantViolated,
        }
    }

    #[must_use]
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use musteragent_content_ref::{ContentRef, MediaType, Sha256};
    use musteragent_identity::ContentId;

    use super::{ContentStoreError, verify_content};

    #[test]
    fn only_unavailable_content_is_eligible_for_dependency_recovery() {
        use musteragent_runtime_contract::ErrorCode;
        let transient = ContentStoreError::Unavailable;
        assert!(transient.retryable());
        assert_eq!(transient.runtime_code(), ErrorCode::ContentUnavailable);
        for error in [
            ContentStoreError::NotFound,
            ContentStoreError::Unauthorized,
            ContentStoreError::TooLarge,
            ContentStoreError::LengthMismatch,
            ContentStoreError::HashMismatch,
            ContentStoreError::IdentityConflict,
            ContentStoreError::Invariant,
        ] {
            assert!(!error.retryable());
            assert_ne!(error.runtime_code(), transient.runtime_code());
        }
    }

    #[test]
    fn immutable_bytes_must_match_length_and_hash() {
        let content = ContentRef {
            content_id: ContentId::generate(),
            media_type: MediaType::new("application/json").expect("media type"),
            byte_length: 2,
            sha256: Sha256::digest(b"{}"),
        };
        assert_eq!(verify_content(&content, b"{}"), Ok(()));
        assert_eq!(
            verify_content(&content, b"{ }"),
            Err(ContentStoreError::LengthMismatch)
        );
        assert_eq!(
            verify_content(&content, b"[]"),
            Err(ContentStoreError::HashMismatch)
        );
    }
}
