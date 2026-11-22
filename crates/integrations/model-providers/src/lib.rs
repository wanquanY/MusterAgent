//! Direct HTTPS model-provider adapters.
//!
//! This crate is the only boundary allowed to know provider wire formats,
//! credential header shapes, Reqwest, SSE, and AWS SigV4.

#![forbid(unsafe_code)]

mod adapter;
mod auth;
mod content;
mod endpoint;
mod failure_metadata;
mod mapping;
pub mod native;
mod normalize;
mod reasoning;
mod recovery;
mod request_transport;
mod response_timing;
mod route_parameters;
mod structured_output;
mod tool_schema;
mod transport_memory;
mod wire;
mod wire_body;
pub use wire_body::{MAX_WIRE_BLOCK_BYTES, NativeWireBody, WIRE_BLOCK_LAYOUT_VERSION};

pub use adapter::{DirectModelProvider, ProviderAdapterConfig};
pub use auth::{
    AwsCredentialMaterial, CredentialMaterial, CredentialResolver, CredentialResolverError,
};
pub use content::{
    ContentBatchFuture, ContentResolver, ContentResolverError, ResolvedContent, ResolvedContents,
};
pub use endpoint::{
    EndpointFuture, EndpointRegistry, EndpointRegistryError, ProviderEndpoint,
    ProviderEndpointOrigin, StaticEndpointRegistry, VersionedEndpointFuture,
};
pub use mapping::{MappingError, map_request};
pub use normalize::overflow::{
    ContextOverflowSource, ExplicitContextOverflow, classify_http_error,
    classify_stream_error_event, classify_terminal_stop_reason,
};
pub use normalize::{NormalizeError, ProviderNormalizer};
pub use recovery::{
    DirectProviderRecoveryProfile, DirectProviderWireProtocol, ProviderRecoveryCapability,
};
pub use request_transport::{
    FirstPartyPrepareFuture, FirstPartyRequestTransport, FirstPartySendFuture,
    PreparedFirstPartyRequest, PreparedModelHttpRequest,
};

mod host_headers;
pub use host_headers::{HostRequestHeaders, ModelRequestHeaderHook};

mod existing_invocation;
pub use existing_invocation::{
    AlreadyAcceptedResponse, ExistingInvocationLookupPolicy, ExistingInvocationRecovery,
    ExistingInvocationRequest, ExistingInvocationResolution, ExistingInvocationResolver,
    HttpExistingInvocationResolver,
};
