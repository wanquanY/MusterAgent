//! Deterministic object storage for Runtime content bytes.
//!
//! The adapter owns bucket/key mechanics. Public Runtime contracts only carry
//! `ContentRef`; even internal diagnostics redact the locator.

#![forbid(unsafe_code)]

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use bytes::Bytes;
use musteragent_runtime_content::{
    BlobIdentity, BlobPutDisposition, BlobStore, BlobStoreError, MAX_BUFFERED_CONTENT_BYTES,
    MAX_CONTENT_BYTES, PutBlob, PutBlobReceipt, StorageLocator,
};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _, PutMode};
use thiserror::Error;

mod probe;
mod s3;
mod staging;
mod stream;

use probe::{Publication, WriteOnceChecks};
pub use s3::WriteOnceStrategy;

const DEFAULT_PREFIX: &str = "musteragent-content-v1";
#[derive(Clone)]
pub struct ObjectStoreBlobStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    segment_bytes: u64,
    s3_checks: Option<Arc<WriteOnceChecks>>,
}

impl Debug for ObjectStoreBlobStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreBlobStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBlobStore {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
    ) -> Result<Self, ObjectStoreConfigError> {
        let prefix = normalized_prefix(prefix.into())?;
        Ok(Self {
            store,
            prefix,
            segment_bytes: stream::SEGMENT_BYTES,
            s3_checks: None,
        })
    }

    pub fn in_memory() -> Self {
        Self {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: DEFAULT_PREFIX.into(),
            segment_bytes: stream::SEGMENT_BYTES,
            s3_checks: None,
        }
    }

    /// Test seam: exercises the segmented layout without multi-GiB fixtures.
    #[cfg(test)]
    fn with_segment_bytes(mut self, segment_bytes: u64) -> Self {
        self.segment_bytes = segment_bytes;
        self
    }

    pub fn from_local_filesystem(
        root: impl AsRef<std::path::Path>,
        prefix: impl Into<String>,
    ) -> Result<Self, ObjectStoreConfigError> {
        let store = object_store::local::LocalFileSystem::new_with_prefix(root)
            .map_err(|_| ObjectStoreConfigError::Build)?
            .with_fsync(true);
        Self::new(Arc::new(store), prefix)
    }

    pub fn from_s3(config: S3ObjectStoreConfig) -> Result<Self, ObjectStoreConfigError> {
        config.validate()?;
        let store = s3::build(&config)?;
        let mut adapter = Self::new(Arc::new(store), config.prefix)?;
        adapter.s3_checks = Some(Arc::default());
        Ok(adapter)
    }

    fn locator(&self, identity: BlobIdentity) -> Result<StorageLocator, BlobStoreError> {
        Self::locator_for_prefix(&self.prefix, identity)
    }

    fn locator_for_prefix(
        prefix: &str,
        identity: BlobIdentity,
    ) -> Result<StorageLocator, BlobStoreError> {
        StorageLocator::new(format!(
            "{}/{}/{}",
            prefix, identity.tenant_id, identity.content_id
        ))
    }

    fn verified_path(
        &self,
        identity: BlobIdentity,
        locator: &StorageLocator,
    ) -> Result<Path, BlobStoreError> {
        let current = self.locator(identity)?;
        if locator != &current {
            return Err(BlobStoreError::LocatorMismatch);
        }
        Path::parse(locator.as_str()).map_err(|_| BlobStoreError::InvalidLocator)
    }

    async fn read_verified(
        &self,
        identity: BlobIdentity,
        locator: &StorageLocator,
    ) -> Result<Vec<u8>, BlobStoreError> {
        let path = self.verified_path(identity, locator)?;
        let object = self.store.get(&path).await.map_err(map_read_error)?;
        // Whole-object reads stay buffered-size only; larger content (and any
        // segmented object) is reachable solely through `copy_to`.
        if object.meta.size > MAX_BUFFERED_CONTENT_BYTES {
            return Err(BlobStoreError::TooLarge);
        }
        let bytes = object.bytes().await.map_err(map_read_error)?;
        if bytes.len() as u64 > MAX_BUFFERED_CONTENT_BYTES {
            return Err(BlobStoreError::TooLarge);
        }
        Ok(bytes.to_vec())
    }
}

impl BlobStore for ObjectStoreBlobStore {
    fn put_stream<'a>(
        &'a self,
        identity: BlobIdentity,
        length: u64,
        hash: musteragent_content_ref::Sha256,
        source: &'a mut dyn musteragent_runtime_content::ContentSource,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<PutBlobReceipt, musteragent_runtime_content::ContentStoreError>,
    > {
        Box::pin(async move { self.put_stream_inner(identity, length, hash, source).await })
    }
    fn copy_to<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
        length: u64,
        hash: musteragent_content_ref::Sha256,
        sink: &'a mut dyn musteragent_runtime_content::ContentSink,
    ) -> musteragent_runtime_content::ApplicationFuture<
        'a,
        Result<(), musteragent_runtime_content::ContentStoreError>,
    > {
        Box::pin(async move {
            self.copy_stream_inner(identity, locator, length, hash, sink)
                .await
        })
    }
    fn put(
        &self,
        request: PutBlob,
    ) -> musteragent_runtime_content::ApplicationFuture<'_, Result<PutBlobReceipt, BlobStoreError>>
    {
        Box::pin(async move {
            if request.bytes.len() as u64 > MAX_BUFFERED_CONTENT_BYTES {
                return Err(BlobStoreError::TooLarge);
            }
            if musteragent_content_ref::Sha256::digest(&request.bytes) != request.sha256 {
                return Err(BlobStoreError::IdentityConflict);
            }
            let locator = self.locator(request.identity)?;
            let path = self.verified_path(request.identity, &locator)?;
            self.check_write_once(Publication::Put)
                .await
                .map_err(|_| BlobStoreError::Unavailable)?;
            let result = self
                .store
                .put_opts(
                    &path,
                    Bytes::from(request.bytes).into(),
                    PutMode::Create.into(),
                )
                .await;
            let disposition = match result {
                Ok(_) => BlobPutDisposition::Created,
                Err(object_store::Error::AlreadyExists { .. }) => {
                    let existing = self.read_verified(request.identity, &locator).await?;
                    if musteragent_content_ref::Sha256::digest(existing) != request.sha256 {
                        return Err(BlobStoreError::IdentityConflict);
                    }
                    BlobPutDisposition::AlreadyPresent
                }
                Err(_) => return Err(BlobStoreError::Unavailable),
            };
            Ok(PutBlobReceipt {
                locator,
                disposition,
            })
        })
    }

    fn read<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> musteragent_runtime_content::ApplicationFuture<'a, Result<Vec<u8>, BlobStoreError>> {
        Box::pin(async move { self.read_verified(identity, locator).await })
    }

    fn delete<'a>(
        &'a self,
        identity: BlobIdentity,
        locator: &'a StorageLocator,
    ) -> musteragent_runtime_content::ApplicationFuture<'a, Result<(), BlobStoreError>> {
        Box::pin(async move {
            let path = self.verified_path(identity, locator)?;
            self.delete_all(&path).await
        })
    }

    fn derive_locator(&self, identity: BlobIdentity) -> Result<StorageLocator, BlobStoreError> {
        self.locator(identity)
    }

    fn validate_locator(
        &self,
        identity: BlobIdentity,
        locator: &StorageLocator,
    ) -> Result<(), BlobStoreError> {
        self.verified_path(identity, locator).map(|_| ())
    }
}

/// Secret-bearing production composition input. Debug is intentionally not
/// implemented.
pub struct S3ObjectStoreConfig {
    /// Atomic publication protocol implemented by the configured bucket.
    pub write_once: WriteOnceStrategy,
    pub region: String,
    pub allow_http: bool,
    pub virtual_hosted_style: bool,
    pub bucket: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub prefix: String,
}

impl S3ObjectStoreConfig {
    fn validate(&self) -> Result<(), ObjectStoreConfigError> {
        if self.bucket.trim().is_empty() {
            return Err(ObjectStoreConfigError::MissingLocation);
        }
        if self.access_key_id.trim().is_empty() || self.secret_access_key.trim().is_empty() {
            return Err(ObjectStoreConfigError::MissingCredentials);
        }
        let url =
            url::Url::parse(&self.endpoint).map_err(|_| ObjectStoreConfigError::InvalidEndpoint)?;
        if self.region.trim().is_empty()
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !(url.scheme() == "https" || (self.allow_http && url.scheme() == "http"))
        {
            return Err(ObjectStoreConfigError::InvalidEndpoint);
        }
        normalized_prefix(self.prefix.clone()).map(|_| ())
    }
}

fn normalized_prefix(value: String) -> Result<String, ObjectStoreConfigError> {
    let value = value.trim_matches('/').to_owned();
    if value.is_empty()
        || value.len() > 256
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b'\\')
        || value.split('/').any(|part| part.is_empty() || part == "..")
    {
        return Err(ObjectStoreConfigError::InvalidPrefix);
    }
    Ok(value)
}

fn map_read_error(error: object_store::Error) -> BlobStoreError {
    match error {
        object_store::Error::NotFound { .. } => BlobStoreError::NotFound,
        _ => BlobStoreError::Unavailable,
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum ObjectStoreConfigError {
    #[error("S3 bucket is required")]
    MissingLocation,
    #[error("S3 access key and secret key are required")]
    MissingCredentials,
    #[error("S3 endpoint or region is invalid; HTTP requires explicit opt-in")]
    InvalidEndpoint,
    #[error("write-once header must be a dedicated HTTP extension header")]
    InvalidWriteOnceHeader,
    #[error("object storage prefix is invalid")]
    InvalidPrefix,
    #[error("object storage client could not be constructed")]
    Build,
}

#[cfg(test)]
mod tests {

    use musteragent_content_ref::Sha256;
    use musteragent_identity::{ContentId, TenantId};
    use musteragent_runtime_content::{BlobIdentity, BlobPutDisposition, BlobStore, PutBlob};

    use super::{ObjectStoreBlobStore, S3ObjectStoreConfig};

    #[tokio::test]
    async fn immutable_create_is_idempotent_and_tenant_separated() {
        let store = ObjectStoreBlobStore::in_memory();
        let identity = BlobIdentity {
            tenant_id: TenantId::generate(),
            content_id: ContentId::generate(),
        };
        let bytes = b"immutable".to_vec();
        let request = PutBlob {
            identity,
            sha256: Sha256::digest(&bytes),
            bytes: bytes.clone(),
        };
        let first = store.put(request.clone()).await.expect("first put");
        assert_eq!(first.disposition, BlobPutDisposition::Created);
        let second = store.put(request).await.expect("duplicate put");
        assert_eq!(second.disposition, BlobPutDisposition::AlreadyPresent);
        assert_eq!(
            store.read(identity, &first.locator).await.expect("read"),
            bytes
        );

        let other = BlobIdentity {
            tenant_id: TenantId::generate(),
            content_id: identity.content_id,
        };
        assert!(store.read(other, &first.locator).await.is_err());
    }

    #[test]
    fn rejects_unowned_prefix_even_when_tenant_and_content_ids_match() {
        let current = ObjectStoreBlobStore::in_memory();
        let identity = BlobIdentity {
            tenant_id: TenantId::generate(),
            content_id: ContentId::generate(),
        };
        let unowned = ObjectStoreBlobStore::locator_for_prefix("unowned-runtime-v1", identity)
            .expect("syntactically valid locator");

        assert!(current.validate_locator(identity, &unowned).is_err());
    }

    #[test]
    fn s3_accepts_independent_endpoints_and_requires_explicit_http() {
        for endpoint in [
            "https://s3.us-east-1.amazonaws.com",
            "https://storage.example.org",
            "http://127.0.0.1:9000",
        ] {
            let config = |allow_http| S3ObjectStoreConfig {
                region: "us-east-1".into(),
                allow_http,
                virtual_hosted_style: false,
                write_once: Default::default(),
                bucket: "bucket".into(),
                endpoint: endpoint.into(),
                access_key_id: "test".into(),
                secret_access_key: "test-secret".into(),
                prefix: "runtime".into(),
            };
            assert_eq!(
                ObjectStoreBlobStore::from_s3(config(false)).is_ok(),
                endpoint.starts_with("https:")
            );
            assert!(ObjectStoreBlobStore::from_s3(config(true)).is_ok());
        }
    }
}
