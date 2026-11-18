use musteragent_artifact::ArtifactSource;
use musteragent_artifact::ArtifactVersionAuthor;
use std::sync::Arc;

use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactVersion, FileRepresentation,
    GeneratedFileContract, file_capability,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ArtifactId, ArtifactVersionId, ResourceChangeSetId, ResourceNodeId, ResourceRevision,
    VersionNumber,
};
use musteragent_resource::ResourceName;
use musteragent_runtime_capability::CapabilityResult;
use musteragent_runtime_content::{ArtifactContentAuthorization, ContentStore, PutContent};
use musteragent_runtime_resource::{PreparedResourceFilePlacement, ResourceMutationIdentity};
use serde::{Deserialize, Serialize};

use crate::{ArtifactMutation, ArtifactReader, ArtifactToolContext, ArtifactToolError};

const MAX_FILE_READ_CHARACTERS: usize = 50_000;
const DEFAULT_FILE_READ_CHARACTERS: usize = 20_000;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FileToolPreparation {
    Commit {
        result: CapabilityResult,
        mutation: Box<ArtifactMutation>,
        resource_placement: Option<Box<PreparedResourceFilePlacement>>,
    },
    ReadOnly(CapabilityResult),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GeneratedFile {
    pub filename: String,
    pub content_ref: ContentRef,
}

pub struct BuiltinFileTools<A> {
    content: Arc<dyn ContentStore>,
    artifacts: Arc<A>,
    artifact_content: Arc<dyn ArtifactContentAuthorization>,
    resource_authority: Option<Arc<dyn musteragent_runtime_resource::ResourceContextAuthority>>,
}

impl<A> Clone for BuiltinFileTools<A> {
    fn clone(&self) -> Self {
        Self {
            content: Arc::clone(&self.content),
            artifacts: Arc::clone(&self.artifacts),
            artifact_content: Arc::clone(&self.artifact_content),
            resource_authority: self.resource_authority.clone(),
        }
    }
}

impl<A> BuiltinFileTools<A>
where
    A: ArtifactReader,
{
    #[must_use]
    pub fn new(
        content: Arc<dyn ContentStore>,
        artifacts: Arc<A>,
        artifact_content: Arc<dyn ArtifactContentAuthorization>,
    ) -> Self {
        Self {
            content,
            artifacts,
            artifact_content,
            resource_authority: None,
        }
    }

    #[must_use]
    pub fn with_resource_authority(
        mut self,
        authority: Arc<dyn musteragent_runtime_resource::ResourceContextAuthority>,
    ) -> Self {
        self.resource_authority = Some(authority);
        self
    }

    async fn placement(
        &self,
        context: &ArtifactToolContext,
        name: &str,
        expected: Option<ResourceRevision>,
        parent: Option<ResourceNodeId>,
    ) -> Result<PreparedResourceFilePlacement, ArtifactToolError> {
        let mut placement = resource_placement(context, name, expected, parent)?;
        if let Some(authority) = &self.resource_authority {
            let mut resolved = authority
                .resolve(musteragent_runtime_resource::ResolveResourceContext {
                    actor: context.invocation.actor,
                    conversation_id: context.invocation.conversation_id,
                    invocation_id: Some(context.invocation.invocation_id),
                    now: context.now,
                })
                .await
                .map_err(|_| ArtifactToolError::InvalidInput)?;
            if resolved.actor != context.invocation.actor
                || resolved.conversation_id() != Some(context.invocation.conversation_id)
                || resolved.workspace_id() != context.invocation.workspace_id
            {
                return Err(ArtifactToolError::InvalidInput);
            }
            resolved.invocation_id = Some(context.invocation.invocation_id);
            resolved.tool_key = Some(
                musteragent_identity::VersionedKey::new(context.invocation.tool_key.as_str())
                    .map_err(|_| ArtifactToolError::InvalidIdentifier)?,
            );
            if matches!(
                resolved.source,
                musteragent_runtime_resource::ResourceCommandSource::DialogueExecution { .. }
                    | musteragent_runtime_resource::ResourceCommandSource::DeviceExecution { .. }
            ) && context.invocation.tool_key.logical_key() == "resource.file.create"
                && !musteragent_runtime_resource::supports_agent_resource_tool(
                    context.invocation.tool_key.as_str(),
                )
            {
                return Err(ArtifactToolError::UnsupportedTool);
            }
            placement.context = resolved;
        }
        Ok(placement)
    }

    pub async fn prepare(
        &self,
        context: ArtifactToolContext,
    ) -> Result<FileToolPreparation, ArtifactToolError> {
        let request = self
            .content
            .read(
                context.invocation.tenant_id,
                &context.invocation.request_ref,
            )
            .await?;
        match context.invocation.tool_key.logical_key() {
            "artifact.file.create" | "resource.file.create" => self.create(context, &request).await,
            "artifact.file.read" => self.read(context, &request).await,
            "artifact.file.patch" => self.patch(context, &request).await,
            _ => Err(ArtifactToolError::UnsupportedTool),
        }
    }

    pub async fn prepare_native_file(
        &self,
        context: ArtifactToolContext,
        capture: &dyn crate::NativeFileCapture,
    ) -> Result<(FileToolPreparation, Vec<ContentRef>), ArtifactToolError> {
        if context.invocation.tool_key.as_str() != "device.workspace.file.publish@1" {
            return Err(ArtifactToolError::UnsupportedTool);
        }
        let input = self
            .content
            .read(
                context.invocation.tenant_id,
                &context.invocation.request_ref,
            )
            .await?;
        let captured = capture.capture(&context, &input).await?;
        validate_filename(&captured.filename)?;
        let reference = captured.content_ref;
        // Stream-verify the complete captured bytes, reporting activity as
        // they pass, instead of spooling a multi-GB private copy.
        self.content
            .copy_to(
                context.invocation.tenant_id,
                &reference,
                &mut crate::VerifyingSink(crate::ContentProgress::new(context.execution.clone())),
            )
            .await?;
        let mutation =
            create_file_mutation(&context, captured.filename.clone(), reference.clone())?;
        let placement = self
            .placement(&context, &captured.filename, None, None)
            .await?;
        let result = self.store_result(&context, &serde_json::json!({
            "artifact_id": mutation.artifact_id().to_string(), "filename": captured.filename,
            "media_type": reference.media_type.as_str(), "byte_length": reference.byte_length,
            "sha256": reference.sha256.to_string(), "status":"ready",
        })).await?;
        Ok((
            FileToolPreparation::Commit {
                result,
                mutation: Box::new(mutation),
                resource_placement: Some(Box::new(placement)),
            },
            vec![reference],
        ))
    }

    pub async fn prepare_generated(
        &self,
        context: ArtifactToolContext,
        file: GeneratedFile,
    ) -> Result<FileToolPreparation, ArtifactToolError> {
        validate_filename(&file.filename)?;
        let contract = GeneratedFileContract::for_tool(context.invocation.capability_key.as_str())
            .ok_or(ArtifactToolError::UnsupportedTool)?;
        if !contract.accepts(&file.filename, &file.content_ref.media_type)
            || !contract.accepts_length(file.content_ref.byte_length)
        {
            return Err(ArtifactToolError::InvalidInput);
        }
        let bytes = self
            .content
            .read(context.invocation.tenant_id, &file.content_ref)
            .await?;
        musteragent_runtime_content::verify_content(&file.content_ref, &bytes)
            .map_err(|_| ArtifactToolError::InvalidInput)?;
        let mutation =
            create_file_mutation(&context, file.filename.clone(), file.content_ref.clone())?;
        let result = self
            .store_result(
                &context,
                &FileWriteOutput::new(&mutation, &file.filename, &file.content_ref),
            )
            .await?;
        Ok(FileToolPreparation::Commit {
            result,
            mutation: Box::new(mutation),
            resource_placement: Some(Box::new(
                self.placement(&context, &file.filename, None, None).await?,
            )),
        })
    }

    /// Replace the bytes of an already resolved managed file. Resource authority
    /// is rechecked by the committer; preparation never creates a second file.
    pub async fn prepare_generated_replacement(
        &self,
        context: ArtifactToolContext,
        target: &musteragent_runtime_resource::ProjectFileVersion,
        content_ref: ContentRef,
    ) -> Result<FileToolPreparation, ArtifactToolError> {
        let contract = GeneratedFileContract::for_tool(context.invocation.capability_key.as_str())
            .ok_or(ArtifactToolError::UnsupportedTool)?;
        if content_ref.media_type != target.content_ref.media_type
            || !contract.accepts(&target.name, &content_ref.media_type)
            || !contract.accepts_length(content_ref.byte_length)
        {
            return Err(ArtifactToolError::InvalidInput);
        }
        let bytes = self
            .content
            .read(context.invocation.tenant_id, &content_ref)
            .await?;
        musteragent_runtime_content::verify_content(&content_ref, &bytes)
            .map_err(|_| ArtifactToolError::InvalidInput)?;
        let mut mutation = update_file_mutation(
            &context,
            target.artifact_id,
            target.version,
            target.name.clone(),
            content_ref.clone(),
        )?;
        if let ArtifactMutation::Update { title, .. } = &mut mutation {
            *title = None;
        }
        let result = self
            .store_result(
                &context,
                &FileWriteOutput::new(&mutation, &target.name, &content_ref),
            )
            .await?;
        Ok(FileToolPreparation::Commit {
            result,
            mutation: Box::new(mutation),
            resource_placement: None,
        })
    }

    async fn create(
        &self,
        context: ArtifactToolContext,
        request: &[u8],
    ) -> Result<FileToolPreparation, ArtifactToolError> {
        let input: FileCreateInput = decode(request)?;
        let canonical =
            input.validate(musteragent_runtime_resource::resource_tool_format_contract(
                context.invocation.tool_key.as_str(),
            ))?;
        let media_type = MediaType::new(input.media_type.clone())
            .map_err(|_| ArtifactToolError::InvalidInput)?;
        let profile = file_capability(&media_type);
        if profile.representation != FileRepresentation::Text
            || !profile.supports_direct_text()
            || !profile.accepts_path(&canonical.path)
        {
            return Err(ArtifactToolError::InvalidInput);
        }
        let bytes = input.content.into_bytes();
        if bytes.is_empty() || bytes.len() as u64 > profile.maximum_bytes {
            return Err(ArtifactToolError::InvalidInput);
        }
        let content_ref = self
            .content
            .put(PutContent {
                tenant_id: context.invocation.tenant_id,
                media_type,
                bytes,
            })
            .await?;
        let mutation = create_file_mutation(&context, canonical.path.clone(), content_ref.clone())?;
        let output = FileWriteOutput::new(&mutation, &canonical.path, &content_ref);
        let result = if musteragent_runtime_resource::resource_tool_format_contract(
            context.invocation.tool_key.as_str(),
        ) == "resource.file.create@2"
        {
            self.store_result(&context, &FileCreateOutput::from(output))
                .await?
        } else {
            self.store_result(&context, &output).await?
        };
        Ok(FileToolPreparation::Commit {
            result,
            mutation: Box::new(mutation),
            resource_placement: Some(Box::new(
                self.placement(
                    &context,
                    &canonical.path,
                    canonical.expected_revision,
                    canonical.parent_node_id,
                )
                .await?,
            )),
        })
    }

    async fn read(
        &self,
        context: ArtifactToolContext,
        request: &[u8],
    ) -> Result<FileToolPreparation, ArtifactToolError> {
        let input: FileReadInput = decode(request)?;
        let artifact_id = parse_artifact_id(&input.file_id)?;
        let view = self.load(&context, artifact_id).await?;
        if view.artifact.kind != ArtifactKind::File {
            return Err(ArtifactToolError::ArtifactKindMismatch);
        }
        let version = select_version(&view, input.version)?;
        self.authorize(&context, &version.content_ref).await?;
        let profile = file_capability(&version.content_ref.media_type);
        let maximum = input
            .maximum_characters
            .unwrap_or(DEFAULT_FILE_READ_CHARACTERS)
            .min(MAX_FILE_READ_CHARACTERS);
        if maximum == 0 {
            return Err(ArtifactToolError::InvalidInput);
        }
        let content = if profile.representation == FileRepresentation::Text {
            let bytes = self
                .content
                .read(context.invocation.tenant_id, &version.content_ref)
                .await?;
            let text = String::from_utf8(bytes).map_err(|_| ArtifactToolError::InvalidInput)?;
            Some(text.chars().take(maximum).collect::<String>())
        } else {
            None
        };
        let result = self
            .store_result(
                &context,
                &FileReadOutput {
                    file_id: artifact_id.to_string(),
                    version: version.version.get(),
                    filename: view.artifact.title.clone(),
                    media_type: version.content_ref.media_type.as_str(),
                    byte_length: version.content_ref.byte_length,
                    sha256: version.content_ref.sha256.to_string(),
                    content,
                },
            )
            .await?;
        Ok(FileToolPreparation::ReadOnly(result))
    }

    async fn patch(
        &self,
        context: ArtifactToolContext,
        request: &[u8],
    ) -> Result<FileToolPreparation, ArtifactToolError> {
        let input: FilePatchInput = decode(request)?;
        input.validate()?;
        let artifact_id = parse_artifact_id(&input.file_id)?;
        let view = self.load(&context, artifact_id).await?;
        if view.artifact.kind != ArtifactKind::File
            || view.artifact.current_version.get() != input.expected_version
        {
            return Err(ArtifactToolError::VersionConflict);
        }
        let current = view
            .versions
            .iter()
            .find(|version| version.version == view.artifact.current_version)
            .ok_or(ArtifactToolError::ArtifactUnavailable)?;
        self.authorize(&context, &current.content_ref).await?;
        let profile = file_capability(&current.content_ref.media_type);
        if profile.representation != FileRepresentation::Text
            || !profile.supports_direct_text()
            || input.content.len() as u64 > profile.maximum_bytes
        {
            return Err(ArtifactToolError::InvalidInput);
        }
        let replacement = self
            .content
            .put(PutContent {
                tenant_id: context.invocation.tenant_id,
                media_type: current.content_ref.media_type.clone(),
                bytes: input.content.into_bytes(),
            })
            .await?;
        if replacement.sha256 == current.content_ref.sha256 {
            return Err(ArtifactToolError::InvalidInput);
        }
        let filename = input
            .filename
            .unwrap_or_else(|| view.artifact.title.clone());
        validate_filename(&filename)?;
        if !profile.accepts_path(&filename) {
            return Err(ArtifactToolError::InvalidInput);
        }
        let mutation = update_file_mutation(
            &context,
            view.artifact.artifact_id,
            current.version,
            filename.clone(),
            replacement.clone(),
        )?;
        let result = self
            .store_result(
                &context,
                &FileWriteOutput::new(&mutation, &filename, &replacement),
            )
            .await?;
        Ok(FileToolPreparation::Commit {
            result,
            mutation: Box::new(mutation),
            resource_placement: None,
        })
    }

    async fn load(
        &self,
        context: &ArtifactToolContext,
        artifact_id: ArtifactId,
    ) -> Result<crate::ArtifactView, ArtifactToolError> {
        self.artifacts
            .load_for_conversation(
                context.invocation.tenant_id,
                context.invocation.conversation_id,
                artifact_id,
            )
            .await
            .map_err(|_| ArtifactToolError::ArtifactUnavailable)?
            .ok_or(ArtifactToolError::ArtifactUnavailable)
    }

    async fn authorize(
        &self,
        context: &ArtifactToolContext,
        reference: &ContentRef,
    ) -> Result<(), ArtifactToolError> {
        if self
            .artifact_content
            .is_current_content_reachable(
                context.invocation.actor,
                context.invocation.conversation_id,
                reference,
            )
            .await
            .map_err(|_| ArtifactToolError::AuthorizationUnavailable)?
        {
            Ok(())
        } else {
            Err(ArtifactToolError::ArtifactUnavailable)
        }
    }

    async fn store_result<T: Serialize>(
        &self,
        context: &ArtifactToolContext,
        value: &T,
    ) -> Result<CapabilityResult, ArtifactToolError> {
        let bytes = serde_json::to_vec(value).map_err(|_| ArtifactToolError::InvalidJson)?;
        let put = PutContent {
            tenant_id: context.invocation.tenant_id,
            media_type: MediaType::new("application/json")
                .map_err(|_| ArtifactToolError::InvalidJson)?,
            bytes,
        };
        let result_ref = self.content.put(put).await?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: context.invocation.result_schema.clone(),
            provider_request_ref: None,
        })
    }
}

fn create_file_mutation(
    context: &ArtifactToolContext,
    title: String,
    content_ref: ContentRef,
) -> Result<ArtifactMutation, ArtifactToolError> {
    let artifact_id = ArtifactId::from_uuid(context.invocation.invocation_id.into_uuid())
        .map_err(|_| ArtifactToolError::InvalidIdentifier)?;
    let version = VersionNumber::new(1).map_err(|_| ArtifactToolError::VersionConflict)?;
    Ok(ArtifactMutation::Create {
        artifact: Artifact {
            artifact_id,
            tenant_id: context.invocation.tenant_id,
            source: ArtifactSource::Conversation(context.invocation.conversation_id),
            origin: ArtifactOrigin::Runtime {
                activity_id: context.activity_id,
                created_by_run_id: context.invocation.run_id,
            },
            kind: ArtifactKind::File,
            lifecycle: ArtifactLifecycle::Draft,
            current_version: version,
            title,
            created_at: context.now,
            updated_at: context.now,
        },
        version: ArtifactVersion {
            artifact_version_id: ArtifactVersionId::from_uuid(
                context.invocation.invocation_id.into_uuid(),
            )
            .map_err(|_| ArtifactToolError::InvalidIdentifier)?,
            artifact_id,
            version,
            content_hash: content_ref.sha256,
            content_ref,
            dependency_refs: Vec::new(),
            source_invocation_id: Some(context.invocation.invocation_id),
            committed_by: ArtifactVersionAuthor::Participant(context.committed_by),
            committed_at: context.now,
        },
    })
}

fn resource_placement(
    context: &ArtifactToolContext,
    filename: &str,
    expected_revision: Option<ResourceRevision>,
    parent_node_id: Option<ResourceNodeId>,
) -> Result<PreparedResourceFilePlacement, ArtifactToolError> {
    let path = filename
        .split('/')
        .map(ResourceName::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ArtifactToolError::InvalidInput)?;
    let sort_key = path
        .last()
        .ok_or(ArtifactToolError::InvalidInput)?
        .comparison_key()
        .to_owned();
    let node_id = ResourceNodeId::from_uuid(context.invocation.invocation_id.into_uuid())
        .map_err(|_| ArtifactToolError::InvalidIdentifier)?;
    let change_set_id =
        ResourceChangeSetId::from_uuid(context.invocation.invocation_id.into_uuid())
            .map_err(|_| ArtifactToolError::InvalidIdentifier)?;
    let mut fingerprint = context.invocation.tenant_id.into_uuid().as_bytes().to_vec();
    fingerprint.extend_from_slice(context.invocation.conversation_id.into_uuid().as_bytes());
    if let Some(workspace_id) = context.invocation.workspace_id {
        fingerprint.extend_from_slice(workspace_id.into_uuid().as_bytes());
    }
    for segment in &path {
        fingerprint.extend_from_slice(segment.as_str().as_bytes());
        fingerprint.push(0);
    }
    let placement = PreparedResourceFilePlacement {
        context: context.resource_context()?,
        identity: ResourceMutationIdentity {
            change_set_id,
            idempotency_key: context.invocation.idempotency_key.clone(),
            command_fingerprint: Sha256::digest(fingerprint),
        },
        expected_revision,
        node_id,
        parent_node_id,
        path,
        sort_key,
    };
    placement
        .validate()
        .map_err(|_| ArtifactToolError::InvalidInput)?;
    Ok(placement)
}

fn update_file_mutation(
    context: &ArtifactToolContext,
    artifact_id: ArtifactId,
    expected_version: VersionNumber,
    title: String,
    content_ref: ContentRef,
) -> Result<ArtifactMutation, ArtifactToolError> {
    let next = expected_version
        .get()
        .checked_add(1)
        .and_then(|value| VersionNumber::new(value).ok())
        .ok_or(ArtifactToolError::VersionConflict)?;
    Ok(ArtifactMutation::Update {
        artifact_id,
        expected_version,
        version: ArtifactVersion {
            artifact_version_id: ArtifactVersionId::from_uuid(
                context.invocation.invocation_id.into_uuid(),
            )
            .map_err(|_| ArtifactToolError::InvalidIdentifier)?,
            artifact_id,
            version: next,
            content_hash: content_ref.sha256,
            content_ref,
            dependency_refs: Vec::new(),
            source_invocation_id: Some(context.invocation.invocation_id),
            committed_by: ArtifactVersionAuthor::Participant(context.committed_by),
            committed_at: context.now,
        },
        title: Some(title),
    })
}

fn select_version(
    view: &crate::ArtifactView,
    requested: Option<u64>,
) -> Result<&ArtifactVersion, ArtifactToolError> {
    let version = requested.unwrap_or_else(|| view.artifact.current_version.get());
    view.versions
        .iter()
        .find(|candidate| candidate.version.get() == version)
        .ok_or(ArtifactToolError::ArtifactUnavailable)
}

fn parse_artifact_id(value: &str) -> Result<ArtifactId, ArtifactToolError> {
    value
        .parse()
        .map_err(|_| ArtifactToolError::InvalidIdentifier)
}

fn validate_filename(value: &str) -> Result<(), ArtifactToolError> {
    let valid = !value.is_empty()
        && value.len() <= 512
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains('\\')
        && !value.chars().any(char::is_control)
        && value
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..");
    if valid {
        Ok(())
    } else {
        Err(ArtifactToolError::InvalidInput)
    }
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ArtifactToolError> {
    serde_json::from_slice(bytes).map_err(|_| ArtifactToolError::InvalidJson)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileCreateInput {
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    parent_node_id: Option<String>,
    #[serde(default)]
    expected_revision: Option<u64>,
    media_type: String,
    content: String,
}

impl FileCreateInput {
    fn validate(&self, tool_key: &str) -> Result<CanonicalFileCreate, ArtifactToolError> {
        let logical_key = tool_key
            .split_once('@')
            .map_or(tool_key, |(logical, _)| logical);
        let resource_tool = logical_key == "resource.file.create";
        let path = match (&self.filename, &self.path, resource_tool) {
            (Some(filename), None, false) => filename.clone(),
            (None, Some(path), true) => path.clone(),
            _ => return Err(ArtifactToolError::InvalidInput),
        };
        validate_filename(&path)?;
        let expected_revision = self.expected_revision.map(ResourceRevision::new);
        if tool_key == "resource.file.create@1" && expected_revision.is_none() {
            return Err(ArtifactToolError::InvalidInput);
        }
        let parent_node_id = self
            .parent_node_id
            .as_deref()
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| ArtifactToolError::InvalidIdentifier)
            })
            .transpose()?;
        // A stable parent identity and a path prefix are two competing ways
        // to express ancestry. Canonical Resource Tools use the stable node
        // identity, so the path must be exactly the leaf name in that mode.
        if parent_node_id.is_some() && path.contains('/') {
            return Err(ArtifactToolError::InvalidInput);
        }
        if !resource_tool && (expected_revision.is_some() || parent_node_id.is_some()) {
            return Err(ArtifactToolError::InvalidInput);
        }
        Ok(CanonicalFileCreate {
            path,
            parent_node_id,
            expected_revision,
        })
    }
}

struct CanonicalFileCreate {
    path: String,
    parent_node_id: Option<ResourceNodeId>,
    expected_revision: Option<ResourceRevision>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileReadInput {
    file_id: String,
    #[serde(default)]
    version: Option<u64>,
    #[serde(default)]
    maximum_characters: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePatchInput {
    file_id: String,
    expected_version: u64,
    content: String,
    #[serde(default)]
    filename: Option<String>,
}

impl FilePatchInput {
    fn validate(&self) -> Result<(), ArtifactToolError> {
        if self.expected_version == 0 || self.content.is_empty() {
            return Err(ArtifactToolError::InvalidInput);
        }
        if let Some(filename) = &self.filename {
            validate_filename(filename)?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct FileWriteOutput<'a> {
    file_id: String,
    file_node_id: String,
    version: u64,
    filename: &'a str,
    media_type: &'a str,
    byte_length: u64,
    sha256: String,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct FileCreateOutput<'a> {
    file_node_id: String,
    filename: &'a str,
    media_type: &'a str,
    byte_length: u64,
    sha256: String,
    status: &'static str,
}

impl<'a> From<FileWriteOutput<'a>> for FileCreateOutput<'a> {
    fn from(value: FileWriteOutput<'a>) -> Self {
        Self {
            file_node_id: value.file_node_id,
            filename: value.filename,
            media_type: value.media_type,
            byte_length: value.byte_length,
            sha256: value.sha256,
            status: value.status,
        }
    }
}

impl<'a> FileWriteOutput<'a> {
    fn new(mutation: &ArtifactMutation, filename: &'a str, reference: &'a ContentRef) -> Self {
        Self {
            file_id: mutation.artifact_id().to_string(),
            file_node_id: mutation.artifact_id().to_string(),
            version: mutation.version().version.get(),
            filename,
            media_type: reference.media_type.as_str(),
            byte_length: reference.byte_length,
            sha256: reference.sha256.to_string(),
            status: "ready",
        }
    }
}

#[derive(Debug, Serialize)]
struct FileReadOutput<'a> {
    file_id: String,
    version: u64,
    filename: String,
    media_type: &'a str,
    byte_length: u64,
    sha256: String,
    content: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARENT_NODE_ID: &str = "018f4f70-894a-7f4c-a10c-ab77453d3ae9";

    fn input() -> FileCreateInput {
        FileCreateInput {
            filename: None,
            path: None,
            parent_node_id: None,
            expected_revision: None,
            media_type: "text/plain".to_owned(),
            content: "hello".to_owned(),
        }
    }

    #[test]
    fn legacy_resource_create_requires_path_and_revision() {
        let mut value = input();
        value.path = Some("README.md".to_owned());
        value.expected_revision = Some(7);
        value.parent_node_id = Some(PARENT_NODE_ID.to_owned());

        let canonical = value
            .validate("resource.file.create@1")
            .expect("valid Resource create");
        assert_eq!(canonical.path, "README.md");
        assert_eq!(canonical.expected_revision, Some(ResourceRevision::new(7)));
        assert_eq!(
            canonical.parent_node_id.map(|id| id.to_string()),
            Some(PARENT_NODE_ID.to_owned())
        );
    }

    #[test]
    fn legacy_artifact_create_keeps_filename_contract() {
        let mut value = input();
        value.filename = Some("reports/result.csv".to_owned());

        let canonical = value
            .validate("artifact.file.create")
            .expect("valid legacy create");
        assert_eq!(canonical.path, "reports/result.csv");
        assert_eq!(canonical.expected_revision, None);
        assert_eq!(canonical.parent_node_id, None);
    }

    #[test]
    fn canonical_resource_create_rejects_ambiguous_or_unfenced_input() {
        let mut mixed = input();
        mixed.filename = Some("legacy.txt".to_owned());
        mixed.path = Some("canonical.txt".to_owned());
        mixed.expected_revision = Some(1);
        assert!(matches!(
            mixed.validate("resource.file.create@2"),
            Err(ArtifactToolError::InvalidInput)
        ));

        let mut unfenced = input();
        unfenced.path = Some("canonical.txt".to_owned());
        assert!(matches!(
            unfenced.validate("resource.file.create@1"),
            Err(ArtifactToolError::InvalidInput)
        ));
        assert!(unfenced.validate("resource.file.create@2").is_ok());
    }

    #[test]
    fn stable_parent_accepts_only_a_leaf_name() {
        let mut value = input();
        value.path = Some("nested/README.md".to_owned());
        value.parent_node_id = Some(PARENT_NODE_ID.to_owned());
        value.expected_revision = Some(4);

        assert!(matches!(
            value.validate("resource.file.create@2"),
            Err(ArtifactToolError::InvalidInput)
        ));
    }
}
