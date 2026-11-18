//! Host-loaded key material for durable kernel identities and content protection.
//! Deployment and external capability provider identities are supplied by hosts.

#![forbid(unsafe_code)]

mod execution_grant_keys;
mod opaque_keys;

pub use execution_grant_keys::{
    EXECUTION_GRANT_KEYRING_SCHEMA_VERSION, ExecutionGrantKeyringConfigError,
    ExecutionGrantKeyringMaterial, load_execution_grant_keyring,
};
pub use opaque_keys::{
    OPAQUE_KEYRING_SCHEMA_VERSION, OpaqueKeyringConfigError, OpaqueKeyringMaterial,
    load_opaque_keyring,
};
