//! Read-only Product identity checks. A target is not an Actor, authority
//! revision or permission grant; clients must not manufacture a query context.
use crate::*;
use musteragent_identity::{ConversationId, WorkspaceId};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileQueryTarget {
    Conversation(ConversationId),
    Workspace(WorkspaceId),
    OwnedAgent(musteragent_identity::AgentId),
}
impl FileQueryTarget {
    pub(crate) fn matches(self, reference: &FileReference) -> bool {
        match self {
            Self::OwnedAgent(id) => {
                reference.owned_agent_id.as_deref() == Some(id.to_string().as_str())
                    && reference.conversation_id.is_none()
                    && reference.workspace_id.is_none()
                    && reference.scope == FileScope::OwnedAgent
            }
            Self::Conversation(id) => {
                reference.conversation_id.as_deref() == Some(id.to_string().as_str())
                    && reference.workspace_id.is_none()
                    && reference.owned_agent_id.is_none()
            }
            Self::Workspace(id) => {
                reference.workspace_id.as_deref() == Some(id.to_string().as_str())
                    && reference.conversation_id.is_none()
                    && reference.owned_agent_id.is_none()
                    && reference.scope == FileScope::Project
            }
        }
    }
}
impl FileQueryContext {
    pub fn target(&self) -> Result<FileQueryTarget, FileQueryError> {
        self.validate()?;
        self.dialogue_source
            .as_ref()
            .map(|source| FileQueryTarget::OwnedAgent(source.agent_id))
            .or_else(|| self.conversation_id.map(FileQueryTarget::Conversation))
            .or_else(|| self.project_workspace_id.map(FileQueryTarget::Workspace))
            .ok_or(FileQueryError::Denied)
    }
}
impl FileEntry {
    pub fn validate_target(&self, target: FileQueryTarget) -> Result<(), FileQueryError> {
        self.file_ref.validate_target(target)?;
        if self.production.as_ref().is_some_and(|production| {
            !production.is_valid() || self.is_directory
                || !matches!(self.file_ref.source, FileSource::Resource { .. })
                || !matches!(target, FileQueryTarget::OwnedAgent(agent) if production.agent_id == agent.to_string())
        }) {
            return Err(FileQueryError::Integrity);
        }
        let native = matches!(
            self.file_ref.source,
            FileSource::Native { .. }
                | FileSource::NativeDocument { .. }
                | FileSource::NativeCreation { .. }
        );
        if native {
            let workspace: WorkspaceId = self
                .native_workspace_id
                .as_deref()
                .ok_or(FileQueryError::Integrity)?
                .parse()
                .map_err(|_| FileQueryError::Integrity)?;
            if self.native_workspace_id.as_deref() != Some(workspace.to_string().as_str())
                || matches!(target, FileQueryTarget::Workspace(id) if id != workspace)
            {
                return Err(FileQueryError::Integrity);
            }
        } else if self.native_workspace_id.is_some() {
            return Err(FileQueryError::Integrity);
        }
        if !text(&self.name, 16384) {
            return Err(FileQueryError::Integrity);
        }
        Ok(())
    }
    pub fn validate_inspection(
        &self,
        target: FileQueryTarget,
        reference: &FileReference,
    ) -> Result<(), FileQueryError> {
        reference.validate_target(target)?;
        self.validate_target(target)?;
        if self.file_ref != *reference {
            return Err(FileQueryError::Integrity);
        }
        Ok(())
    }

    /// Explicit metadata refresh may advance only the revision, never the
    /// source, scope or target. Content descriptors still use exact inspection.
    pub fn validate_refreshed_inspection(
        &self,
        target: FileQueryTarget,
        reference: &FileReference,
    ) -> Result<(), FileQueryError> {
        reference.validate_target(target)?;
        self.validate_target(target)?;
        let mut observed = self.file_ref.clone();
        observed.revision.clone_from(&reference.revision);
        if observed != *reference {
            return Err(FileQueryError::Integrity);
        }
        Ok(())
    }
}
impl FilePage {
    pub fn validate_target(
        &self,
        target: FileQueryTarget,
        query: &ListFiles,
    ) -> Result<(), FileQueryError> {
        query.validate_target(target)?;
        if query.produced_by.as_ref().is_some_and(|filter| {
            self.entries.iter().any(|entry| {
                entry
                    .production
                    .as_ref()
                    .is_none_or(|value| !filter.matches(value))
            })
        }) {
            return Err(FileQueryError::Integrity);
        }
        if self.entries.len() > query.limit
            || !text(&self.revision, 512)
            || self
                .next_cursor
                .as_ref()
                .is_some_and(|cursor| !text(cursor, 4096))
            || self.next_cursor.is_some()
                && ((self.entries.is_empty() && self.coverage != FileCoverage::OwnedAgentFiles)
                    || self.next_cursor == query.cursor)
            || !matches!(
                (query.scope, self.coverage),
                (
                    FileScope::Conversation,
                    FileCoverage::ProjectedConversation | FileCoverage::RestrictedConversation
                ) | (FileScope::Project, FileCoverage::ProjectChildren)
                    | (
                        FileScope::OwnedAgent,
                        FileCoverage::OwnedAgentResources | FileCoverage::OwnedAgentFiles
                    )
            )
            || self.retraction_status.is_some()
                && self.coverage != FileCoverage::ProjectedConversation
        {
            return Err(FileQueryError::Integrity);
        }
        let mut seen = BTreeSet::new();
        for entry in &self.entries {
            entry.validate_target(target)?;
            if entry.file_ref.scope != query.scope
                || !seen.insert((&entry.file_ref.source, &entry.file_ref.revision))
            {
                return Err(FileQueryError::Integrity);
            }
        }
        Ok(())
    }
}
impl FileContentDescriptor {
    pub fn validate_target(
        &self,
        target: FileQueryTarget,
        reference: &FileReference,
    ) -> Result<(), FileQueryError> {
        self.entry.validate_inspection(target, reference)?;
        if self.entry.is_directory || self.dependencies.len() > MAX_INDEPENDENT_FILE_DEPENDENCIES {
            return Err(FileQueryError::Integrity);
        }
        if let Some(origin) = &self.artifact_origin {
            match &reference.source {
                FileSource::Artifact { artifact_id }
                    if *artifact_id == origin.artifact_id.to_string() => {}
                FileSource::Resource { .. } => {}
                _ => return Err(FileQueryError::Integrity),
            }
        }
        let mut seen = BTreeSet::new();
        for reference in std::iter::once(&self.primary).chain(&self.dependencies) {
            if reference.byte_length > musteragent_runtime_content::MAX_CONTENT_BYTES
                || !seen.insert(reference.content_id)
            {
                return Err(FileQueryError::Integrity);
            }
        }
        Ok(())
    }
}
fn text(value: &str, limit: usize) -> bool {
    !value.is_empty() && value.len() <= limit && !value.chars().any(char::is_control)
}
