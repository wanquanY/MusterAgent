//! Project/Conversation resource-tree, versioned-file and durable-edit invariants.

#![forbid(unsafe_code)]

mod edit;
mod name;
mod tree;
mod versioned_file;

pub use edit::{
    EditSession, EditSessionState, TextAnchor, TextEditOperation, TextRange, apply_text_operations,
};
pub use name::{MAX_RESOURCE_NAME_BYTES, ResourceName, ResourceNameError};
pub use tree::{
    ResourceNode, ResourceNodeKind, ResourceNodeLifecycle, ResourceProvenance, ResourceScope,
    ResourceTreeError,
};
pub use versioned_file::{
    VERSIONED_FILE_SCHEMA, VersionedFileContractError, VersionedFileDocument,
};
