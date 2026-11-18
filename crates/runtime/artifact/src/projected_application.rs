//! Product output queries share the current Conversation projection's visibility.
//!
//! The Artifact catalog retains immutable versions for project files, history and
//! explicit reads. It cannot decide whether a Runtime output still belongs to the
//! active conversation after a semantic rewind or a context-branch switch.

use std::{collections::BTreeSet, sync::Arc};

use musteragent_artifact::ArtifactOrigin;
use musteragent_identity::{ArtifactId, CorrelationId, DottedKey};
use musteragent_projection::{ConversationProjectionApplication, GetConversationProjection};
use musteragent_runtime_contract::{
    ApplicationFuture, ErrorCode, ErrorDetails, RUNTIME_ERROR_SCHEMA_VERSION, RuntimeError,
    SafeDisplayText,
};

use crate::{
    ArtifactApplication, ArtifactPage, ArtifactView, GetArtifact, ListConversationArtifacts,
};

/// Filters Runtime artifact listings through the current conversation projection.
pub struct ProjectedArtifactApplication {
    catalog: Arc<dyn ArtifactApplication>,
    projections: Arc<dyn ConversationProjectionApplication>,
}

impl ProjectedArtifactApplication {
    /// Composes the authenticated artifact catalog with current branch visibility.
    #[must_use]
    pub fn new(
        catalog: Arc<dyn ArtifactApplication>,
        projections: Arc<dyn ConversationProjectionApplication>,
    ) -> Self {
        Self {
            catalog,
            projections,
        }
    }

    async fn list(&self, query: ListConversationArtifacts) -> Result<ArtifactPage, RuntimeError> {
        query
            .validate()
            .map_err(|_| failure(ErrorCode::InvalidCommand, "Invalid Artifact page query"))?;
        let mut cursor = query.before_artifact_id;
        let mut visible: Option<BTreeSet<ArtifactId>> = None;
        let mut artifacts = Vec::with_capacity(query.limit + 1);
        loop {
            // Ask the storage adapter to apply authorization, search and kind
            // constraints first. Fill the public page *after* visibility filtering.
            let page = self
                .catalog
                .list_conversation_artifacts(ListConversationArtifacts {
                    before_artifact_id: cursor,
                    ..query.clone()
                })
                .await?;
            if visible.is_none()
                && page
                    .artifacts
                    .iter()
                    .any(|item| matches!(item.artifact.origin, ArtifactOrigin::Runtime { .. }))
            {
                let snapshot = self
                    .projections
                    .get_head_projection(GetConversationProjection {
                        actor: query.actor,
                        conversation_id: query.conversation_id,
                        preferred_snapshot_id: None,
                    })
                    .await?;
                if snapshot.tenant_id != query.actor.tenant_id
                    || snapshot.conversation_id != query.conversation_id
                {
                    return Err(invalid("Artifact projection scope mismatch"));
                }
                visible = Some(
                    snapshot
                        .artifacts
                        .into_iter()
                        .map(|item| item.artifact_id)
                        .collect(),
                );
            }
            for item in page.artifacts {
                if item.artifact.tenant_id != query.actor.tenant_id
                    || item.artifact.source.conversation_id() != Some(query.conversation_id)
                {
                    return Err(invalid("Artifact catalog scope mismatch"));
                }
                // Independent imports and directly authored files have no Run
                // to rewind. They also serve read-only archives without a native
                // Conversation projection; never fabricate one to list them.
                if !matches!(item.artifact.origin, ArtifactOrigin::Runtime { .. })
                    || visible
                        .as_ref()
                        .is_some_and(|ids| ids.contains(&item.artifact.artifact_id))
                {
                    artifacts.push(item);
                    if artifacts.len() > query.limit {
                        artifacts.pop();
                        return Ok(ArtifactPage {
                            next_before_artifact_id: artifacts
                                .last()
                                .map(|item| item.artifact.artifact_id),
                            artifacts,
                            has_more: true,
                        });
                    }
                }
            }
            if !page.has_more {
                return Ok(ArtifactPage {
                    artifacts,
                    next_before_artifact_id: None,
                    has_more: false,
                });
            }
            let next = page
                .next_before_artifact_id
                .ok_or_else(|| invalid("Artifact catalog omitted its next cursor"))?;
            if cursor.is_some_and(|previous| next >= previous) {
                return Err(invalid("Artifact catalog cursor did not advance"));
            }
            cursor = Some(next);
        }
    }
}

impl ArtifactApplication for ProjectedArtifactApplication {
    fn get_artifact(
        &self,
        query: GetArtifact,
    ) -> ApplicationFuture<'_, Result<ArtifactView, RuntimeError>> {
        self.catalog.get_artifact(query)
    }

    fn list_conversation_artifacts(
        &self,
        query: ListConversationArtifacts,
    ) -> ApplicationFuture<'_, Result<ArtifactPage, RuntimeError>> {
        Box::pin(self.list(query))
    }
}

fn invalid(message: &'static str) -> RuntimeError {
    failure(ErrorCode::InternalInvariantViolated, message)
}

fn failure(code: ErrorCode, message: &'static str) -> RuntimeError {
    RuntimeError {
        schema_version: DottedKey::new(RUNTIME_ERROR_SCHEMA_VERSION).expect("static error schema"),
        code,
        category: code.category(),
        correlation_id: CorrelationId::generate(),
        retryable: false,
        message: SafeDisplayText::new(message).expect("static error message"),
        retry_after_ms: None,
        resource_ref: None,
        details: Box::new(ErrorDetails::None),
    }
}
