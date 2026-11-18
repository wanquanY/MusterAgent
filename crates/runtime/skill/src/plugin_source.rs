//! Public immutable plugin publication material, independent of account grants.
use musteragent_content_ref::Sha256;
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

use crate::ProductPluginRelease;

/// Failures at the host's authenticated publication boundary.
#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum PluginReleaseSourceError {
    #[error("plugin publication source is unavailable")]
    Unavailable,
    #[error("plugin publication was not found")]
    NotFound,
    #[error("plugin publication identity or digest is corrupt")]
    Corrupt,
}

/// Hosts supply authenticated releases; reading one never grants execution.
pub trait PluginReleaseSource: Send + Sync {
    /// Read exactly this version and verify its digest; never select a newer one.
    fn read(
        &self,
        plugin_id: String,
        version: String,
        digest: Sha256,
    ) -> ApplicationFuture<'_, Result<ProductPluginRelease, PluginReleaseSourceError>>;

    /// List verified publication material, including retained immutable versions.
    fn list(
        &self,
    ) -> ApplicationFuture<'_, Result<Vec<ProductPluginRelease>, PluginReleaseSourceError>>;
}
