//! Shared, authority-preserving file discovery for Product and Agent callers.
//!
//! This crate owns a query view, never file bytes, a new Artifact identity or
//! a second version chain. Project storage is selected by the injected authority.
#![forbid(unsafe_code)]

mod activity;
mod application;
mod authority;
mod service;
pub use authority::{
    CurrentFileQueryAuthority, FileQueryAccess, FileQueryAuthority, FileQueryStateSource,
    RunFileQueryAccess, WorkspaceFileQueryAuthority,
};
pub use service::FileQueryService;
mod artifact_read;
mod content;
mod conversation;
mod cursor;
mod independent;
pub use cursor::{paginate_file_entries, paginate_file_snapshot, paginate_file_snapshot_async};
mod agent_files;
pub use agent_files::{
    AgentFileCatalog, DialogueInputDelegation, DialogueInputPage, DialogueInputPosition,
    DialogueInputSource, DialogueInputView, DialogueOutputSource,
};
mod ports;
mod preview;
mod product_content;
mod product_validation;
pub use product_validation::FileQueryTarget;
mod project;
pub use product_content::{FileContentDescriptor, FileContentRead};
mod representation;
pub use representation::{DocumentRepresentation, UploadedDocumentReader};
mod types;

pub use application::FileQueryApplication;
pub use content::ContentFileReader;
pub use conversation::ConversationFileCatalog;
pub use musteragent_runtime_resource::{
    AgentFileProduction, FileExecutionHome, FileProductionFilter,
};
pub use ports::{
    ConversationFilePlacement, ConversationFilePlacements, ConversationFileRelations,
    ConversationFileSnapshotSource, ConversationFileVersion, ConversationFileVersions,
    FileArtifactVersionReader, FileCatalog, FileContentReader, FileRetractionStatusSource,
    IndependentConversationFileSource, MAX_INDEPENDENT_FILE_ARTIFACTS,
    MAX_INDEPENDENT_FILE_DEPENDENCIES, ProjectFileSourceCoverage, ProjectFileSourceCoverageReader,
    ResolvedArtifactVersion, ResolvedFile,
};
pub use project::ManagedResourceFileCatalog;
pub use types::*;

mod search;
pub use search::{FileSearchGap, FileSearchHit, FileSearchPage, FileSearchTraversal, SearchFiles};

mod source_contract;
mod tool_query;

mod file_tree;
pub use file_tree::{FileTreeDescriptor, FileTreeEntry};
