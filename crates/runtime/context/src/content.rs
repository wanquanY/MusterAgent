//! The planner names the purpose of its output at the point it creates it.
//! The persistence adapter, not a MIME heuristic, owns content access.
use crate::ContextPlanError;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{ContentId, TenantId};
use musteragent_run::AttemptContext;
use musteragent_runtime_content::{
    ContentStore, MAX_BUFFERED_CONTENT_BYTES, PutContent, verify_content,
};
use musteragent_runtime_contract::ApplicationFuture;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextPlanContentKind {
    StructuredSummary,
    ModelRequest,
    CompactionSummary,
    CompactionPartial,
    DialogueSourcePage,
    OwnerSourceManifest,
}
impl ContextPlanContentKind {
    pub fn media_type(self) -> MediaType {
        MediaType::new(match self {
            Self::StructuredSummary => "application/vnd.musteragent.legacy.context-summary+json",
            Self::ModelRequest => musteragent_runtime_model::MODEL_REQUEST_MEDIA_TYPE,
            Self::CompactionSummary => "text/markdown;charset=utf-8",
            Self::CompactionPartial => "application/vnd.musteragent.compaction-partial+text",
            Self::DialogueSourcePage => {
                "application/vnd.musteragent.dialogue-context-page+protobuf"
            }
            Self::OwnerSourceManifest => {
                "application/vnd.musteragent.dialogue-context-cut+protobuf"
            }
        })
        .expect("declared Context media type")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextPlanContentWrite {
    pub tenant_id: TenantId,
    pub kind: ContextPlanContentKind,
    pub content: ContentRef,
    pub bytes: Vec<u8>,
}
impl ContextPlanContentWrite {
    pub fn validate(&self) -> Result<(), ContextPlanError> {
        if self.content.media_type != self.kind.media_type()
            || self.content.byte_length > MAX_BUFFERED_CONTENT_BYTES
        {
            return Err(ContextPlanError::invariant());
        }
        verify_content(&self.content, &self.bytes).map_err(|_| ContextPlanError::invariant())
    }
}

/// Writes only bytes produced by the original Context planner. It cannot
/// grant arbitrary reads or attest a physical model invocation or usage.
pub trait ContextPlanContentWriter: Send + Sync {
    fn put(
        &self,
        attempt: &AttemptContext,
        tenant_id: TenantId,
        kind: ContextPlanContentKind,
        bytes: Vec<u8>,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContextPlanError>>;
}

pub(crate) struct StoredContextPlanContent(pub Arc<dyn ContentStore>);
impl ContextPlanContentWriter for StoredContextPlanContent {
    fn put(
        &self,
        attempt: &AttemptContext,
        tenant_id: TenantId,
        kind: ContextPlanContentKind,
        bytes: Vec<u8>,
    ) -> ApplicationFuture<'_, Result<ContentRef, ContextPlanError>> {
        let identity = ContentId::from_uuid(uuid::Uuid::new_v5(
            &attempt.run_id.into_uuid(),
            format!(
                "context-content@1:{}:{}",
                kind.media_type().as_str(),
                Sha256::digest(&bytes)
            )
            .as_bytes(),
        ));
        Box::pin(async move {
            self.0
                .put_at(
                    identity.map_err(|_| ContextPlanError::invariant())?,
                    PutContent {
                        tenant_id,
                        media_type: kind.media_type(),
                        bytes,
                    },
                )
                .await
                .map_err(|_| ContextPlanError::unavailable())
        })
    }
}
