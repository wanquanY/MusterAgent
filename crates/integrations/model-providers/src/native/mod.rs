//! Shared native provider adapters. Persistence, account lifecycle and network
//! transport are injected by the application; no Desktop/Mobile app dependency.

mod content;
mod credentials;
mod embedding;
mod proxy;
mod token;

pub use content::StoredModelContentResolver;
pub use credentials::{AdmittedCredentialResolver, CredentialMaterialSource};
pub use embedding::{NativeEmbeddingHttpPolicy, NativeMemoryEmbeddingProvider};
pub use proxy::NativeProxyEndpointRegistry;
pub use token::NativeModelCredentialAuthority;

#[cfg(test)]
mod content_tests;
#[cfg(test)]
mod tests;
