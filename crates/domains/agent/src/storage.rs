//! Immutable Agent catalog and publication storage contracts.

use crate::{AgentDefinition, AgentVersion, PublishedAgentVersionPointer};
use musteragent_identity::{AgentId, AgentKey, AgentVersionId, PrincipalId, UtcTimestamp};
use musteragent_runtime_contract::ApplicationFuture;
use thiserror::Error;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DefinitionCommitDisposition {
    Created,
    Unchanged,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum VersionCommitDisposition {
    Created,
    Unchanged,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommitAgentVersion {
    pub agent_key: AgentKey,
    pub version: AgentVersion,
}

/// The minimum expected state required to advance a Published Pointer.
/// Both values are mandatory so a stale publisher cannot win by observing only
/// the Agent version or only the pointer revision.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct PublishedAgentVersionGuard {
    pub agent_version_id: AgentVersionId,
    pub pointer_version: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublishAgentVersion {
    pub agent_key: AgentKey,
    pub agent_version_id: AgentVersionId,
    pub expected: Option<PublishedAgentVersionGuard>,
    pub published_at: UtcTimestamp,
    pub published_by: PrincipalId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResolvedPublishedAgentVersion {
    pub definition: AgentDefinition,
    pub version: AgentVersion,
    pub pointer: PublishedAgentVersionPointer,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResolvedPinnedAgentVersion {
    pub definition: AgentDefinition,
    pub version: AgentVersion,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DefinitionInsertOutcome {
    Inserted,
    Existing(AgentDefinition),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum VersionInsertOutcome {
    Inserted,
    Existing(Box<AgentVersion>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PublishedPointerCasOutcome {
    Swapped(PublishedAgentVersionPointer),
    Conflict(Option<PublishedAgentVersionPointer>),
}

/// Persistence contract for the Agent Definition Context.
///
/// Implementations must atomically enforce definition identity uniqueness,
/// immutable version IDs and per-Agent version numbers, and the pointer CAS.
/// No method permits updating an existing definition or AgentVersion in place.
pub trait AgentDefinitionStore: Send + Sync {
    fn load_definition<'a>(
        &'a self,
        agent_key: &'a AgentKey,
    ) -> ApplicationFuture<'a, Result<Option<AgentDefinition>, AgentDefinitionStoreError>>;

    fn insert_definition(
        &self,
        definition: AgentDefinition,
    ) -> ApplicationFuture<'_, Result<DefinitionInsertOutcome, AgentDefinitionStoreError>>;

    fn load_version(
        &self,
        agent_version_id: AgentVersionId,
    ) -> ApplicationFuture<'_, Result<Option<AgentVersion>, AgentDefinitionStoreError>>;

    fn insert_version(
        &self,
        version: AgentVersion,
    ) -> ApplicationFuture<'_, Result<VersionInsertOutcome, AgentDefinitionStoreError>>;

    fn load_published(
        &self,
        agent_id: AgentId,
    ) -> ApplicationFuture<
        '_,
        Result<Option<PublishedAgentVersionPointer>, AgentDefinitionStoreError>,
    >;

    fn compare_and_swap_published(
        &self,
        agent_id: AgentId,
        expected: Option<PublishedAgentVersionGuard>,
        replacement: PublishedAgentVersionPointer,
    ) -> ApplicationFuture<'_, Result<PublishedPointerCasOutcome, AgentDefinitionStoreError>>;
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum AgentDefinitionStoreError {
    #[error("Agent Definition store is unavailable")]
    Unavailable,
    #[error("Agent Definition store returned corrupt or non-canonical data")]
    Corrupt,
}
