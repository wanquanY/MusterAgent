use musteragent_content_ref::MediaType;
use musteragent_identity::UtcTimestamp;
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

pub const MAX_REMOTE_IMAGE_BYTES: u64 = 20 * 1024 * 1024;
const MAX_REMOTE_URL_BYTES: usize = 4_000;

/// An untrusted public URL fetch request owned by a Rust Capability.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RemoteImageFetchRequest {
    pub url: String,
    pub max_bytes: u64,
    pub deadline: UtcTimestamp,
}

impl RemoteImageFetchRequest {
    pub fn validate(&self) -> Result<(), RemoteImageFetchError> {
        if self.url.is_empty()
            || self.url.len() > MAX_REMOTE_URL_BYTES
            || self.url.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(RemoteImageFetchError::InvalidUrl);
        }
        if self.max_bytes == 0 || self.max_bytes > MAX_REMOTE_IMAGE_BYTES {
            return Err(RemoteImageFetchError::InvalidSizeLimit);
        }
        Ok(())
    }
}

/// Immutable bytes returned only after URL, DNS, redirect and media checks.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchedImage {
    pub final_url: String,
    pub media_type: MediaType,
    pub bytes: Vec<u8>,
}

impl FetchedImage {
    pub fn validate(&self, request: &RemoteImageFetchRequest) -> Result<(), RemoteImageFetchError> {
        if self.final_url.is_empty()
            || self.final_url.len() > MAX_REMOTE_URL_BYTES
            || self.bytes.is_empty()
            || self.bytes.len() as u64 > request.max_bytes
            || !self.media_type.as_str().starts_with("image/")
        {
            return Err(RemoteImageFetchError::InvalidResponse);
        }
        Ok(())
    }
}

/// Network adapter port. Implementations must pin validated DNS answers for
/// each hop so a second resolver lookup cannot bypass SSRF policy.
pub trait RemoteImageFetcher: Send + Sync {
    fn fetch(
        &self,
        request: RemoteImageFetchRequest,
    ) -> ApplicationFuture<'_, Result<FetchedImage, RemoteImageFetchError>>;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum RemoteImageFetchError {
    #[error("remote image URL is invalid")]
    InvalidUrl,
    #[error("remote image size limit is invalid")]
    InvalidSizeLimit,
    #[error("remote image target is forbidden")]
    ForbiddenTarget,
    #[error("remote image redirect policy was violated")]
    RedirectViolation,
    #[error("remote image response is invalid")]
    InvalidResponse,
    #[error("remote image exceeds the configured size limit")]
    TooLarge,
    #[error("remote image fetch timed out")]
    DeadlineExceeded,
    #[error("remote image transport is unavailable")]
    Unavailable,
}
