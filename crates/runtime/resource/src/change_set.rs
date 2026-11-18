//! Atomic multi-node Resource changes shared by Product and Agent Tool ingress.

use std::collections::BTreeSet;

use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactVersion, FileRepresentation,
    file_capability,
};
use musteragent_identity::{
    ArtifactVersionId, ExecutionWorkspaceId, IdempotencyKey, ParticipantId, ResourceEditSessionId,
    ResourceEditTerminalCommandId, ResourceNodeId, ResourceRevision, VersionNumber,
};
use musteragent_resource::{ResourceName, ResourceNode, ResourceScope};

use crate::{
    ResourceApplicationError, ResourceCommandContext, ResourceMutationIdentity, validate_sort_key,
};

/// Hard transaction bound. A complete workspace change exceeding this bound is
/// rejected before publication. Never split one promised atomic commit into
/// independently visible chunks; imports with separate transactions must say so.
pub const MAX_RESOURCE_CHANGE_SET_MUTATIONS: usize = 64;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ResourceChangeSetMutation {
    CopyWorkTree {
        mutation_key: IdempotencyKey,
        node_id: ResourceNodeId,
        parent_node_id: Option<ResourceNodeId>,
        name: ResourceName,
        source_workspace_id: ExecutionWorkspaceId,
        source_node_id: ResourceNodeId,
        source_revision: ResourceRevision,
        committed_by: ParticipantId,
    },
    SetResult {
        mutation_key: IdempotencyKey,
        node_id: ResourceNodeId,
        is_result: bool,
    },
    /// Whole-file bytes, including binary formats. Uses the original Artifact
    /// identity and exact base version; no synthetic text-edit session.
    ReplaceFile {
        mutation_key: IdempotencyKey,
        file: crate::ProjectFileVersion,
        version: Box<ArtifactVersion>,
    },
    CreateFolder {
        mutation_key: IdempotencyKey,
        node_id: ResourceNodeId,
        parent_node_id: Option<ResourceNodeId>,
        name: ResourceName,
        sort_key: String,
    },
    CreateFile {
        mutation_key: IdempotencyKey,
        node_id: ResourceNodeId,
        parent_node_id: Option<ResourceNodeId>,
        name: ResourceName,
        sort_key: String,
        artifact: Artifact,
        version: Box<ArtifactVersion>,
    },
    PublishEdit {
        mutation_key: IdempotencyKey,
        command_id: ResourceEditTerminalCommandId,
        session_id: ResourceEditSessionId,
        expected_session_revision: ResourceRevision,
        expected_file_version: VersionNumber,
        artifact_version_id: ArtifactVersionId,
        committed_by: ParticipantId,
    },
    Rename {
        mutation_key: IdempotencyKey,
        node_id: ResourceNodeId,
        name: ResourceName,
    },
    Move {
        mutation_key: IdempotencyKey,
        node_id: ResourceNodeId,
        target_parent_node_id: Option<ResourceNodeId>,
        name: Option<ResourceName>,
    },
    Trash {
        mutation_key: IdempotencyKey,
        node_id: ResourceNodeId,
    },
    Restore {
        mutation_key: IdempotencyKey,
        node_id: ResourceNodeId,
        target_parent_node_id: Option<ResourceNodeId>,
        name: Option<ResourceName>,
    },
}

impl ResourceChangeSetMutation {
    #[must_use]
    pub const fn mutation_key(&self) -> &IdempotencyKey {
        match self {
            Self::CopyWorkTree { mutation_key, .. }
            | Self::SetResult { mutation_key, .. }
            | Self::ReplaceFile { mutation_key, .. }
            | Self::CreateFolder { mutation_key, .. }
            | Self::CreateFile { mutation_key, .. }
            | Self::PublishEdit { mutation_key, .. }
            | Self::Rename { mutation_key, .. }
            | Self::Move { mutation_key, .. }
            | Self::Trash { mutation_key, .. }
            | Self::Restore { mutation_key, .. } => mutation_key,
        }
    }

    #[must_use]
    pub const fn result_node_id(&self) -> Option<ResourceNodeId> {
        match self {
            Self::ReplaceFile { file, .. } => Some(file.node_id),
            Self::CopyWorkTree { node_id, .. }
            | Self::SetResult { node_id, .. }
            | Self::CreateFolder { node_id, .. }
            | Self::CreateFile { node_id, .. }
            | Self::Rename { node_id, .. }
            | Self::Move { node_id, .. }
            | Self::Trash { node_id, .. }
            | Self::Restore { node_id, .. } => Some(*node_id),
            Self::PublishEdit { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommitResourceChangeSet {
    pub context: ResourceCommandContext,
    pub identity: ResourceMutationIdentity,
    pub expected_revision: ResourceRevision,
    pub mutations: Vec<ResourceChangeSetMutation>,
}

impl CommitResourceChangeSet {
    pub fn validate(&self) -> Result<(), ResourceApplicationError> {
        self.context.validate()?;
        if self
            .mutations
            .iter()
            .any(|mutation| matches!(mutation, ResourceChangeSetMutation::CopyWorkTree { .. }))
            && (self.mutations.len() != 1
                || self.context.invocation_id.is_some()
                || !matches!(self.context.scope(), ResourceScope::Project(_)))
        {
            return Err(ResourceApplicationError::InvalidFile);
        }
        let maximum = if self.context.invocation_id.is_some()
            && (matches!(
                self.context.source,
                crate::ResourceCommandSource::ExecutionWorkspace(_)
            ) || self
                .context
                .tool_key
                .as_ref()
                .is_some_and(|k| k.as_str() == "data.python.run@11"))
        {
            30_000
        } else {
            MAX_RESOURCE_CHANGE_SET_MUTATIONS
        };
        if self.mutations.is_empty() || self.mutations.len() > maximum {
            return Err(ResourceApplicationError::InvalidChangeSet);
        }
        let mut keys = BTreeSet::new();
        for mutation in &self.mutations {
            if !keys.insert(mutation.mutation_key().as_str()) {
                return Err(ResourceApplicationError::InvalidChangeSet);
            }
            match mutation {
                ResourceChangeSetMutation::ReplaceFile { file, version, .. } => {
                    validate_replacement(&self.context, file, version)?;
                }
                ResourceChangeSetMutation::CreateFolder { sort_key, .. } => {
                    validate_sort_key(sort_key)?;
                }
                ResourceChangeSetMutation::CreateFile {
                    name,
                    sort_key,
                    artifact,
                    version,
                    ..
                } => {
                    validate_sort_key(sort_key)?;
                    validate_file(&self.context, name, artifact, version)?;
                }
                ResourceChangeSetMutation::CopyWorkTree { .. }
                | ResourceChangeSetMutation::SetResult { .. }
                | ResourceChangeSetMutation::PublishEdit { .. }
                | ResourceChangeSetMutation::Rename { .. }
                | ResourceChangeSetMutation::Move { .. }
                | ResourceChangeSetMutation::Trash { .. }
                | ResourceChangeSetMutation::Restore { .. } => {}
            }
        }
        Ok(())
    }
}

pub(crate) fn validate_file(
    context: &ResourceCommandContext,
    name: &ResourceName,
    artifact: &Artifact,
    version: &ArtifactVersion,
) -> Result<(), ResourceApplicationError> {
    let capability = file_capability(&version.content_ref.media_type);
    let valid_origin = match artifact.origin {
        ArtifactOrigin::Product => {
            context.invocation_id.is_none() && version.source_invocation_id.is_none()
        }
        ArtifactOrigin::Runtime { .. } => {
            context.invocation_id.is_some() && version.source_invocation_id == context.invocation_id
        }
        ArtifactOrigin::GovernedImport => false,
    };
    if !valid_origin
        || artifact.kind != ArtifactKind::File
        || artifact.lifecycle != ArtifactLifecycle::Draft
        || artifact.tenant_id != context.tenant_id
        || match artifact.source {
            musteragent_artifact::ArtifactSource::Conversation(id) => {
                context.conversation_id() != Some(id)
                    || version.committed_by.participant_id().is_none()
            }
            musteragent_artifact::ArtifactSource::Workspace(id) => {
                context.conversation_id().is_some()
                    || context.workspace_id() != Some(id)
                    || version.committed_by.principal_id() != Some(context.actor.principal_id)
            }
        }
        || artifact.current_version.get() != 1
        || artifact.title != name.as_str()
        || version.artifact_id != artifact.artifact_id
        || version.version.get() != 1
        || version.content_hash != version.content_ref.sha256
        || capability.representation == FileRepresentation::Text
            && !capability.supports_direct_text()
        || !capability.accepts_path(name.as_str())
        || version.content_ref.byte_length > capability.maximum_bytes
    {
        return Err(ResourceApplicationError::InvalidFile);
    }
    artifact
        .validate()
        .map_err(|_| ResourceApplicationError::InvalidFile)?;
    version
        .validate()
        .map_err(|_| ResourceApplicationError::InvalidFile)
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceChangeSetMutationReceipt {
    pub mutation_key: IdempotencyKey,
    pub node: ResourceNode,
    pub artifact_version_id: Option<ArtifactVersionId>,
    pub file_version: Option<VersionNumber>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResourceChangeSetReceipt {
    pub scope: ResourceScope,
    pub previous_revision: ResourceRevision,
    pub revision: ResourceRevision,
    pub mutations: Vec<ResourceChangeSetMutationReceipt>,
    pub duplicate: bool,
}

/// Shared whole-file replacement rules for Product and fenced execution commits.
pub fn validate_replacement(
    context: &ResourceCommandContext,
    file: &crate::ProjectFileVersion,
    version: &ArtifactVersion,
) -> Result<(), ResourceApplicationError> {
    let capability = file_capability(&version.content_ref.media_type);
    let author_matches = match (context.conversation_id(), version.committed_by) {
        (Some(_), musteragent_artifact::ArtifactVersionAuthor::Participant(_)) => true,
        (None, musteragent_artifact::ArtifactVersionAuthor::ProductPrincipal(id)) => {
            id == context.actor.principal_id
        }
        _ => false,
    };
    if !author_matches
        || version.artifact_id != file.artifact_id
        || file.version.get().checked_add(1) != Some(version.version.get())
        || version.artifact_version_id == file.artifact_version_id
        || version.source_invocation_id != context.invocation_id
        || version.content_hash != version.content_ref.sha256
        || version.content_ref.byte_length > capability.maximum_bytes
        || !capability.accepts_path(&file.name)
    {
        return Err(ResourceApplicationError::InvalidFile);
    }
    Ok(())
}
