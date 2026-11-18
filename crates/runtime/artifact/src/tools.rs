use musteragent_artifact::ArtifactSource;
use musteragent_artifact::ArtifactVersionAuthor;
use std::collections::BTreeMap;
use std::sync::Arc;

use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactVersion,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ActivityId, ArtifactId, ArtifactVersionId, ParticipantId, UtcTimestamp, VersionNumber,
};
use musteragent_runtime_capability::{CapabilityInvocation, CapabilityResult};
use musteragent_runtime_content::{ContentStore, ContentStoreError, PutContent};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::ArtifactMutation;

const MAX_TITLE_CHARS: usize = 500;

pub struct ArtifactToolContext {
    pub execution: Option<musteragent_runtime_capability::OperationContext>,
    pub invocation: CapabilityInvocation,
    pub activity_id: ActivityId,
    pub committed_by: ParticipantId,
    pub now: UtcTimestamp,
}

impl ArtifactToolContext {
    pub(crate) fn resource_context(
        &self,
    ) -> Result<musteragent_runtime_resource::ResourceCommandContext, ArtifactToolError> {
        use musteragent_runtime_resource::{ResourceCommandContext, ResourceCommandSource};
        Ok(ResourceCommandContext {
            tenant_id: self.invocation.tenant_id,
            actor: self.invocation.actor,
            source: ResourceCommandSource::Conversation {
                conversation_id: self.invocation.conversation_id,
                workspace_id: self.invocation.workspace_id,
            },
            invocation_id: Some(self.invocation.invocation_id),
            tool_key: Some(
                musteragent_identity::VersionedKey::new(self.invocation.tool_key.as_str())
                    .map_err(|_| ArtifactToolError::InvalidIdentifier)?,
            ),
            now: self.now,
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BackendMediaSourceKind {
    ImageGenerationRecord,
    VideoGenerationRecord,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BackendMediaSource {
    pub kind: BackendMediaSourceKind,
    pub source_id: String,
    pub task_id: String,
    pub title: String,
    pub prompt: String,
    pub asset_urls: Vec<String>,
    pub assets: Vec<BackendMediaAsset>,
    pub thumbnail_url: Option<String>,
    pub provider_request_ref: Option<String>,
    pub provider_model: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BackendMediaAsset {
    pub content_ref: ContentRef,
    pub delivery_url: Option<String>,
    pub dimensions: Option<(u32, u32)>,
}

/// Validation port invoked by the Runtime Artifact owner before it persists a
/// public Tool result. The production adapter compiles the Snapshot-pinned
/// Draft 2020-12 schema with remote resolution disabled.
pub trait ArtifactOutputSchemaValidator: Send + Sync {
    fn validate(
        &self,
        schema_hash: Sha256,
        schema_bytes: &[u8],
        instance: &Value,
    ) -> Result<(), ArtifactOutputSchemaValidationError>;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ArtifactOutputSchemaValidationError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ArtifactToolPreparation {
    Commit {
        result: CapabilityResult,
        mutation: Box<ArtifactMutation>,
    },
    ReadOnly(CapabilityResult),
}

/// Prepares immutable media artifacts and their governed tool results.
#[derive(Clone)]
pub struct BuiltinArtifactTools {
    content: Arc<dyn ContentStore>,
}

impl BuiltinArtifactTools {
    /// Uses the execution home's immutable content store for staged artifacts.
    #[must_use]
    pub fn new(content: Arc<dyn ContentStore>) -> Self {
        Self { content }
    }

    /// Converts an authenticated named Backend media result into a canonical
    /// Runtime-owned Artifact version. Tool output and Artifact content remain
    /// distinct immutable ContentRefs.
    pub async fn prepare_backend_media(
        &self,
        context: ArtifactToolContext,
        source: BackendMediaSource,
        artifact_type: musteragent_tool::MediaArtifactKind,
        output_schema_json: &[u8],
        schema_validator: &dyn ArtifactOutputSchemaValidator,
    ) -> Result<ArtifactToolPreparation, ArtifactToolError> {
        let (kind, expected_source_kind) = match artifact_type {
            musteragent_tool::MediaArtifactKind::Image => (
                ArtifactKind::Image,
                BackendMediaSourceKind::ImageGenerationRecord,
            ),
            musteragent_tool::MediaArtifactKind::Video => (
                ArtifactKind::Video,
                BackendMediaSourceKind::VideoGenerationRecord,
            ),
        };
        source.validate(expected_source_kind)?;
        for asset in &source.assets {
            let verified = self
                .content
                .open_verified(context.invocation.tenant_id, &asset.content_ref)
                .await?;
            if verified.reference() != &asset.content_ref {
                return Err(ArtifactToolError::InvalidBackendResult);
            }
        }
        let artifact_id = ArtifactId::from_uuid(context.invocation.invocation_id.into_uuid())
            .map_err(|_| ArtifactToolError::InvalidIdentifier)?;
        let output = match kind {
            ArtifactKind::Image => MediaToolOutput::image(artifact_id, &source),
            ArtifactKind::Video => MediaToolOutput::video(artifact_id, &source),
            _ => return Err(ArtifactToolError::UnsupportedTool),
        };
        let output_value =
            serde_json::to_value(&output).map_err(|_| ArtifactToolError::InvalidBackendResult)?;
        schema_validator
            .validate(
                context.invocation.result_schema.schema_hash,
                output_schema_json,
                &output_value,
            )
            .map_err(|_| ArtifactToolError::InvalidBackendResult)?;
        let document = MediaArtifactDocument {
            source_kind: source.kind.as_str().to_owned(),
            source_id: source.source_id.clone(),
            task_id: source.task_id.clone(),
            title: source.title.clone(),
            prompt: source.prompt.clone(),
            asset_urls: source.asset_urls.clone(),
            assets: source
                .assets
                .iter()
                .map(|asset| MediaArtifactAssetDocument {
                    content_ref: StoredContentRef::from(&asset.content_ref),
                    delivery_url: asset.delivery_url.clone(),
                })
                .collect(),
            thumbnail_url: source.thumbnail_url.clone(),
            provider_request_ref: source.provider_request_ref.clone(),
            provider_model: source.provider_model.clone(),
        };
        let dependencies = source
            .assets
            .iter()
            .map(|asset| asset.content_ref.clone())
            .collect();
        let mutation = self
            .create_mutation(
                &context,
                kind,
                source.title.clone(),
                &document,
                dependencies,
            )
            .await?;
        if mutation.artifact_id() != artifact_id {
            return Err(ArtifactToolError::InvalidBackendResult);
        }
        let mut result = self.store_result(&context.invocation, &output).await?;
        result.provider_request_ref = source
            .provider_request_ref
            .map(musteragent_runtime_contract::OpaqueExternalRef::new)
            .transpose()
            .map_err(|_| ArtifactToolError::InvalidBackendResult)?;
        Ok(ArtifactToolPreparation::Commit {
            result,
            mutation: Box::new(mutation),
        })
    }

    async fn create_mutation<T: Serialize>(
        &self,
        context: &ArtifactToolContext,
        kind: ArtifactKind,
        title: String,
        document: &T,
        dependency_refs: Vec<ContentRef>,
    ) -> Result<ArtifactMutation, ArtifactToolError> {
        let artifact_id = ArtifactId::from_uuid(context.invocation.invocation_id.into_uuid())
            .map_err(|_| ArtifactToolError::InvalidIdentifier)?;
        let version_id = ArtifactVersionId::from_uuid(context.invocation.invocation_id.into_uuid())
            .map_err(|_| ArtifactToolError::InvalidIdentifier)?;
        let content_ref = self
            .store_json(
                context.invocation.tenant_id,
                context.invocation.invocation_id,
                "artifact",
                document,
            )
            .await?;
        let version_number =
            VersionNumber::new(1).map_err(|_| ArtifactToolError::VersionConflict)?;
        let mutation = ArtifactMutation::Create {
            artifact: Artifact {
                artifact_id,
                tenant_id: context.invocation.tenant_id,
                source: ArtifactSource::Conversation(context.invocation.conversation_id),
                origin: ArtifactOrigin::Runtime {
                    activity_id: context.activity_id,
                    created_by_run_id: context.invocation.run_id,
                },
                kind,
                lifecycle: ArtifactLifecycle::Draft,
                current_version: version_number,
                title: title.clone(),
                created_at: context.now,
                updated_at: context.now,
            },
            version: ArtifactVersion {
                artifact_version_id: version_id,
                artifact_id,
                version: version_number,
                content_hash: content_ref.sha256,
                content_ref,
                dependency_refs,
                source_invocation_id: Some(context.invocation.invocation_id),
                committed_by: ArtifactVersionAuthor::Participant(context.committed_by),
                committed_at: context.now,
            },
        };
        Ok(mutation)
    }

    async fn store_result<T: Serialize>(
        &self,
        invocation: &CapabilityInvocation,
        output: &T,
    ) -> Result<CapabilityResult, ArtifactToolError> {
        let result_ref = self
            .store_json(
                invocation.tenant_id,
                invocation.invocation_id,
                "result",
                output,
            )
            .await?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: invocation.result_schema.clone(),
            provider_request_ref: None,
        })
    }

    async fn store_json<T: Serialize>(
        &self,
        tenant_id: musteragent_identity::TenantId,
        invocation: musteragent_identity::InvocationId,
        purpose: &str,
        value: &T,
    ) -> Result<ContentRef, ArtifactToolError> {
        let bytes = serde_json::to_vec(value).map_err(|_| ArtifactToolError::InvalidJson)?;
        self.content
            .put_at(
                musteragent_identity::ContentId::from_uuid(uuid::Uuid::new_v5(
                    &invocation.into_uuid(),
                    purpose.as_bytes(),
                ))
                .map_err(|_| ArtifactToolError::InvalidIdentifier)?,
                PutContent {
                    tenant_id,
                    media_type: MediaType::new("application/json")
                        .map_err(|_| ArtifactToolError::InvalidJson)?,
                    bytes,
                },
            )
            .await
            .map_err(Into::into)
    }
}

fn bounded(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.chars().count() <= max
}

#[derive(Debug, Serialize)]
struct ArtifactSummary {
    artifact_id: String,
    artifact_type: &'static str,
    version: u64,
    title: String,
    status: &'static str,
    preview: BTreeMap<String, Value>,
    links: BTreeMap<String, Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StoredContentRef {
    content_id: String,
    media_type: String,
    byte_length: u64,
    sha256: String,
}

impl StoredContentRef {
    pub(crate) fn to_domain(&self) -> Result<ContentRef, ArtifactToolError> {
        Ok(ContentRef {
            content_id: self
                .content_id
                .parse()
                .map_err(|_| ArtifactToolError::InvalidIdentifier)?,
            media_type: MediaType::new(self.media_type.clone())
                .map_err(|_| ArtifactToolError::InvalidInput)?,
            byte_length: self.byte_length,
            sha256: parse_sha256_hex(&self.sha256)?,
        })
    }
}

impl From<&ContentRef> for StoredContentRef {
    fn from(value: &ContentRef) -> Self {
        Self {
            content_id: value.content_id.to_string(),
            media_type: value.media_type.as_str().to_owned(),
            byte_length: value.byte_length,
            sha256: value.sha256.to_string(),
        }
    }
}

impl BackendMediaSourceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ImageGenerationRecord => "image_generation_record",
            Self::VideoGenerationRecord => "video_generation_record",
        }
    }
}

impl BackendMediaSource {
    fn validate(&self, expected_kind: BackendMediaSourceKind) -> Result<(), ArtifactToolError> {
        let has_materializable_assets = match expected_kind {
            BackendMediaSourceKind::ImageGenerationRecord => !self.assets.is_empty(),
            BackendMediaSourceKind::VideoGenerationRecord => !self.assets.is_empty(),
        };
        if self.kind != expected_kind
            || self.source_id.parse::<ArtifactId>().is_err()
            || !bounded(&self.task_id, 100)
            || !bounded(&self.title, MAX_TITLE_CHARS)
            || !bounded(&self.prompt, 8_000)
            || !has_materializable_assets
            || self.asset_urls.len() > 16
            || self
                .asset_urls
                .iter()
                .any(|value| !bounded(value, 8_000) || value.chars().any(char::is_control))
            || self.assets.len() > 16
            || self.assets.iter().any(|asset| {
                !(match expected_kind {
                    BackendMediaSourceKind::ImageGenerationRecord => {
                        asset.content_ref.media_type.as_str().starts_with("image/")
                            && asset.content_ref.byte_length <= 20 * 1024 * 1024
                    }
                    BackendMediaSourceKind::VideoGenerationRecord => matches!(
                        asset.content_ref.media_type.as_str(),
                        "video/mp4" | "video/webm" | "video/quicktime"
                    ),
                }) || asset.content_ref.byte_length == 0
                    || asset.dimensions.is_some_and(|(width, height)| {
                        !(1..=40_000).contains(&width) || !(1..=40_000).contains(&height)
                    })
                    || asset.delivery_url.as_deref().is_some_and(|value| {
                        !bounded(value, 8_000) || value.chars().any(char::is_control)
                    })
            })
            || self
                .thumbnail_url
                .as_deref()
                .is_some_and(|value| !bounded(value, 8_000) || value.chars().any(char::is_control))
            || self.provider_request_ref.as_ref().is_some_and(|value| {
                value.is_empty()
                    || value.len() > 512
                    || !value.bytes().all(|byte| byte.is_ascii_graphic())
            })
            || (expected_kind == BackendMediaSourceKind::VideoGenerationRecord
                && self
                    .provider_model
                    .as_deref()
                    .is_none_or(|value| !bounded(value, 255)))
        {
            return Err(ArtifactToolError::InvalidBackendResult);
        }
        Ok(())
    }
}

#[cfg(test)]
mod canonical_video_tests {
    use super::*;
    #[test]
    fn new_video_commit_requires_original_canonical_bytes_and_accepts_video_sizes() {
        let mut source = BackendMediaSource {
            kind: BackendMediaSourceKind::VideoGenerationRecord,
            source_id: "019fa6de-38fd-7000-8000-000000000708".into(),
            task_id: "video-original".into(),
            title: "Film".into(),
            prompt: "Create the film".into(),
            asset_urls: vec!["https://cdn.example.com/film.mp4".into()],
            assets: Vec::new(),
            thumbnail_url: None,
            provider_request_ref: None,
            provider_model: Some("configured-model".into()),
        };
        assert!(
            source
                .validate(BackendMediaSourceKind::VideoGenerationRecord)
                .is_err()
        );
        source.assets.push(BackendMediaAsset {
            content_ref: ContentRef {
                content_id: musteragent_identity::ContentId::generate(),
                media_type: MediaType::new("video/mp4").expect("valid canonical video fixture"),
                byte_length: 40_000_000,
                sha256: Sha256::digest(b"canonical original video"),
            },
            delivery_url: None,
            dimensions: None,
        });
        assert!(
            source
                .validate(BackendMediaSourceKind::VideoGenerationRecord)
                .is_ok()
        );
        source.assets[0].content_ref.media_type =
            MediaType::new("image/png").expect("valid canonical video fixture");
        assert!(
            source
                .validate(BackendMediaSourceKind::VideoGenerationRecord)
                .is_err()
        );
    }
}

#[derive(Debug, Serialize)]
struct MediaToolOutput {
    success: bool,
    data: Value,
    artifact: ArtifactSummary,
}

impl MediaToolOutput {
    fn image(artifact_id: ArtifactId, source: &BackendMediaSource) -> Self {
        Self {
            success: true,
            data: serde_json::json!({
                "task_id": source.task_id,
                "record_id": source.source_id,
                "status": "completed",
                "message": "Image generation completed and was materialized by the Runtime",
                "prompt": source.prompt,
            }),
            artifact: media_artifact_summary(artifact_id, "image", source),
        }
    }

    fn video(artifact_id: ArtifactId, source: &BackendMediaSource) -> Self {
        Self {
            success: true,
            data: serde_json::json!({
                "task_id": source.task_id,
                "record_id": source.source_id,
                "status": "completed",
                "message": "Video generation completed and was materialized by the Runtime",
                "model_used": source.provider_model.as_deref().unwrap_or_default(),
            }),
            artifact: media_artifact_summary(artifact_id, "video", source),
        }
    }
}

fn media_artifact_summary(
    artifact_id: ArtifactId,
    artifact_type: &'static str,
    source: &BackendMediaSource,
) -> ArtifactSummary {
    // Canonical ContentRefs and delivery URLs describe the same logical assets.
    // Prefer canonical assets when present so the summary never double-counts
    // an image that is represented in both forms during migration.
    let asset_count = if source.assets.is_empty() {
        source.asset_urls.len()
    } else {
        source.assets.len()
    };
    let mut links = BTreeMap::from([(
        "assets".to_owned(),
        Value::Array(
            source
                .asset_urls
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    )]);
    if !source.assets.is_empty() {
        links.insert(
            "content_refs".to_owned(),
            serde_json::to_value(
                source
                    .assets
                    .iter()
                    .map(|asset| StoredContentRef::from(&asset.content_ref))
                    .collect::<Vec<_>>(),
            )
            .expect("StoredContentRef serialization is infallible"),
        );
    }
    if let Some(thumbnail) = &source.thumbnail_url {
        links.insert("thumbnail".to_owned(), Value::String(thumbnail.clone()));
    }
    let mut preview = BTreeMap::from([
        ("asset_count".to_owned(), Value::from(asset_count)),
        (
            "source_id".to_owned(),
            Value::String(source.source_id.clone()),
        ),
    ]);
    let images: Vec<_> = source.assets.iter().filter_map(|asset| {
        asset.dimensions.map(|(width, height)| serde_json::json!({
            "content_id": asset.content_ref.content_id.to_string(), "width": width, "height": height,
        }))
    }).collect();
    if !images.is_empty() {
        preview.insert("images".to_owned(), Value::Array(images));
    }
    ArtifactSummary {
        artifact_id: artifact_id.to_string(),
        artifact_type,
        version: 1,
        title: source.title.clone(),
        status: "ready",
        preview,
        links,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MediaArtifactDocument {
    pub(crate) source_kind: String,
    pub(crate) source_id: String,
    pub(crate) task_id: String,
    title: String,
    prompt: String,
    pub(crate) asset_urls: Vec<String>,
    #[serde(default)]
    pub(crate) assets: Vec<MediaArtifactAssetDocument>,
    thumbnail_url: Option<String>,
    provider_request_ref: Option<String>,
    provider_model: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MediaArtifactAssetDocument {
    content_ref: StoredContentRef,
    delivery_url: Option<String>,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ArtifactToolError {
    /// A capability that is not configured or has no capacity right now.
    #[error("Capability unavailable: {0}")]
    Unavailable(String),
    /// A host reports its own classified, safe-to-display tool failure.
    #[error("{message}")]
    Reported {
        code: musteragent_runtime_contract::ErrorCode,
        retryable: bool,
        message: String,
    },
    #[error("Artifact Tool is not implemented by the Rust built-in provider")]
    UnsupportedTool,
    #[error("Artifact Tool input or stored content is invalid JSON")]
    InvalidJson,
    #[error("Artifact Tool input violates its pinned schema")]
    InvalidInput,
    #[error("invalid argument at {path}: {expected}")]
    InvalidArgument {
        path: String,
        expected: &'static str,
    },
    #[error("Artifact identifier is invalid")]
    InvalidIdentifier,
    #[error("Artifact is unavailable for this tenant")]
    ArtifactUnavailable,
    #[error("Artifact storage is temporarily unavailable")]
    ArtifactStorageUnavailable,
    #[error("Artifact storage returned corrupt state")]
    ArtifactStateCorrupt,
    #[error("Artifact kind does not match the requested Tool")]
    ArtifactKindMismatch,
    #[error("Artifact version has advanced")]
    VersionConflict,
    #[error("requested content is not reachable from the current Run or canonical Artifact")]
    UnauthorizedContent,
    #[error("Artifact content authorization is unavailable")]
    AuthorizationUnavailable,
    #[error("named Backend media result violates the pinned Artifact contract")]
    InvalidBackendResult,
    #[error("Content Store operation failed")]
    Content(#[from] ContentStoreError),
}

fn parse_sha256_hex(value: &str) -> Result<musteragent_content_ref::Sha256, ArtifactToolError> {
    if value.len() != musteragent_content_ref::Sha256::LENGTH * 2 {
        return Err(ArtifactToolError::InvalidInput);
    }
    let mut bytes = [0_u8; musteragent_content_ref::Sha256::LENGTH];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let hex = std::str::from_utf8(chunk).map_err(|_| ArtifactToolError::InvalidInput)?;
        bytes[index] = u8::from_str_radix(hex, 16).map_err(|_| ArtifactToolError::InvalidInput)?;
    }
    musteragent_content_ref::Sha256::from_bytes(bytes).map_err(|_| ArtifactToolError::InvalidInput)
}

/// Media URLs are delivery hints; only the committed dependency closure is readable.
pub(crate) fn image_file_representation(
    version: &musteragent_artifact::ArtifactVersion,
    bytes: &[u8],
) -> Result<crate::ArtifactFileRepresentation, ArtifactToolError> {
    let document: MediaArtifactDocument =
        serde_json::from_slice(bytes).map_err(|_| ArtifactToolError::InvalidJson)?;
    if document.source_kind != BackendMediaSourceKind::ImageGenerationRecord.as_str() {
        return Err(ArtifactToolError::InvalidInput);
    }
    let references = document
        .assets
        .iter()
        .map(|asset| asset.content_ref.to_domain())
        .collect::<Result<Vec<_>, _>>()?;
    if references.is_empty() {
        return Ok(crate::ArtifactFileRepresentation::Unsupported);
    }
    if references.iter().any(|reference| {
        !version.dependency_refs.contains(reference)
            || !matches!(
                reference.media_type.as_str(),
                "image/png" | "image/jpeg" | "image/webp" | "image/gif"
            )
    }) {
        return Err(ArtifactToolError::UnauthorizedContent);
    }
    Ok(crate::ArtifactFileRepresentation::Visual(references))
}
