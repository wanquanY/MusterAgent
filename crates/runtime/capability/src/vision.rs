use std::sync::Arc;
mod admission;
pub use admission::{VisionAdmissionBasis, validate_vision_admission};

use musteragent_content_ref::{ContentRef, MediaType};
use musteragent_identity::{ContentId, InvocationId};
use musteragent_model::{MAX_LOGICAL_VISION_IMAGES, VisionInputProfile};
use musteragent_run::InvocationContext;
use musteragent_runtime_content::{
    ArtifactContentAuthorization, ContentStore, ContentStoreError, PutContent, verify_content,
};
use musteragent_runtime_contract::{ApplicationFuture, OpaqueExternalRef};
use musteragent_runtime_snapshot::{
    ModelRouteSnapshot, ModelStreamBudgetSnapshot, PinnedModelRoute,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{CapabilityInvocation, CapabilityResult, OperationContext};

const MAX_VISION_QUESTION_BYTES: usize = 8_000;
pub(crate) const MAX_VISION_ANALYSIS_BYTES: usize = 100_000;
/// Appended to the analysis when the vision model stopped at its output token
/// limit. The published `vision.analyze` output schema is sealed and closed, so
/// truncation travels in the analysis text the main model reads.
pub(crate) const VISION_TRUNCATION_NOTICE: &str = "[Visual analysis truncated: the vision model reached its output token limit. \
     Ask a narrower question or fewer images for the remaining detail.]";
const MAX_TASK_CONTEXT_BYTES: usize = 32_000;
const NATIVE_MOUNT_MESSAGE: &str =
    "The authorized images are mounted in the current task context for direct visual inspection.";

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VisionDetail {
    #[default]
    Auto,
    Low,
    High,
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VisionAnalysisMode {
    #[default]
    Joint,
    Independent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VisionModelImage {
    pub image_ref: ContentRef,
    pub label: Option<String>,
}

/// Closed routing decision for one visual inspection.
///
/// `NativePrimary` never creates a nested model invocation: the Tool result
/// mounts the exact image ContentRef into the next iteration of the already
/// selected primary model. `AuxiliaryVision` is used only when the primary
/// route cannot consume image pixels and always uses the Run-pinned Admin
/// auxiliary route.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum VisionExecutionMode {
    NativePrimary,
    AuxiliaryVision,
}

impl VisionExecutionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NativePrimary => "native_primary",
            Self::AuxiliaryVision => "auxiliary_vision",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VisionModelInvocation {
    /// Executor-local opt-in derived from the frozen role chain. It never changes the canonical provider request.
    pub capture_provider_failure: bool,
    pub invocation_context: InvocationContext,
    /// Admission-pinned Admin auxiliary route and credential grant. Vision
    /// never resolves a mutable model registry at execution time.
    pub model: PinnedModelRoute,
    pub stream_budget: ModelStreamBudgetSnapshot,
    pub images: Vec<VisionModelImage>,
    pub question: String,
    pub detail: VisionDetail,
    pub analysis_mode: VisionAnalysisMode,
    /// Bounded, hash-verified projection of the current task. This gives the
    /// auxiliary visual model the user's actual objective without pretending
    /// it is another Agent or copying the complete conversation transcript.
    pub task_context: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VisionModelResult {
    pub analysis: String,
    pub resolved_model_key: String,
    pub provider_request_ref: Option<OpaqueExternalRef>,
}

/// Agent-loop-owned model runner. Its adapter records the physical Model
/// Invocation and Usage Fact before returning a semantic analysis.
pub trait VisionModelInvoker: Send + Sync {
    fn analyze_observed(
        &self,
        request: VisionModelInvocation,
        _parent: OperationContext,
    ) -> ApplicationFuture<'_, Result<VisionModelResult, VisionCapabilityError>> {
        Box::pin(async move {
            let _ = request;
            Err(VisionCapabilityError::IdentityMismatch)
        })
    }
    fn analyze(
        &self,
        request: VisionModelInvocation,
    ) -> ApplicationFuture<'_, Result<VisionModelResult, VisionCapabilityError>>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct VisionExecutionRequest {
    pub capability: CapabilityInvocation,
    pub model_invocation_context: InvocationContext,
    /// The exact primary/fallback route active for the iteration that emitted
    /// this Tool call.
    pub primary_model: PinnedModelRoute,
    pub auxiliary_models: Vec<PinnedModelRoute>,
    pub stream_budget: ModelStreamBudgetSnapshot,
    pub task_context_refs: Vec<ContentRef>,
}

#[derive(Clone)]
pub struct BuiltinVisionCapability {
    content: Arc<dyn ContentStore>,
    model: Arc<dyn VisionModelInvoker>,
    artifact_content: Option<Arc<dyn ArtifactContentAuthorization>>,
}

impl BuiltinVisionCapability {
    #[must_use]
    pub fn new(content: Arc<dyn ContentStore>, model: Arc<dyn VisionModelInvoker>) -> Self {
        Self {
            content,
            model,
            artifact_content: None,
        }
    }

    /// Adds exact, request-driven Artifact ContentRef authorization for images
    /// referenced across Runs in the same authenticated Conversation.
    #[must_use]
    pub fn with_artifact_content_authorization(
        mut self,
        authorization: Arc<dyn ArtifactContentAuthorization>,
    ) -> Self {
        self.artifact_content = Some(authorization);
        self
    }

    pub async fn execute(
        &self,
        request: VisionExecutionRequest,
    ) -> Result<CapabilityResult, VisionCapabilityError> {
        self.execute_observed(request, None).await
    }

    pub async fn execute_observed(
        &self,
        request: VisionExecutionRequest,
        parent: Option<OperationContext>,
    ) -> Result<CapabilityResult, VisionCapabilityError> {
        validate_identity(&request)?;
        if let Some(context) = &parent
            && context.stopping()
        {
            return Err(VisionCapabilityError::ModelFailed);
        }
        let tenant_id = request.capability.tenant_id;
        let request_bytes = self
            .content
            .read(tenant_id, &request.capability.request_ref)
            .await?;
        let input: VisionInput = serde_json::from_slice(&request_bytes)
            .map_err(|_| VisionCapabilityError::InvalidInput)?;
        input.validate()?;
        let images = self.resolve_images(&request.capability, &input).await?;
        let mode = if pinned_model_supports_vision(&request.primary_model) {
            VisionExecutionMode::NativePrimary
        } else {
            VisionExecutionMode::AuxiliaryVision
        };
        let (model_result, partitions, degraded_joint_context) = match mode {
            VisionExecutionMode::NativePrimary => {
                let profile = vision_input_profile(&request.primary_model)?;
                validate_image_limits(&images, profile)?;
                let partitions = partition_images(&images, profile)?;
                if partitions.len() > 1 {
                    return Err(match input.analysis_mode {
                        VisionAnalysisMode::Joint => VisionCapabilityError::JointInputLimit,
                        VisionAnalysisMode::Independent => VisionCapabilityError::InputLimit,
                    });
                }
                (
                    VisionModelResult {
                        analysis: NATIVE_MOUNT_MESSAGE.to_owned(),
                        resolved_model_key: request.primary_model.route.resolved_model_key.clone(),
                        provider_request_ref: None,
                    },
                    partitions,
                    false,
                )
            }
            VisionExecutionMode::AuxiliaryVision => {
                self.analyze_candidates(&request, &input, &images, parent.as_ref())
                    .await?
            }
        };
        model_result.validate()?;
        let output = serde_json::to_vec(&VisionOutput {
            analysis: &model_result.analysis,
            image_refs: images
                .iter()
                .map(|image| VisionContentRef::from(&image.image_ref))
                .collect(),
            model_key: &model_result.resolved_model_key,
            mode: mode.as_str(),
            batch: VisionBatchOutput {
                logical_image_count: images.len(),
                provider_invocation_count: partitions.len(),
                partitions: &partitions,
                degraded_joint_context,
            },
        })
        .map_err(|_| VisionCapabilityError::InvalidResult)?;
        let result_ref = self
            .content
            .put(PutContent {
                tenant_id,
                media_type: MediaType::new("application/json")
                    .map_err(|_| VisionCapabilityError::InvalidResult)?,
                bytes: output,
            })
            .await?;
        Ok(CapabilityResult {
            result_hash: result_ref.sha256,
            result_ref,
            result_schema: request.capability.result_schema,
            provider_request_ref: model_result.provider_request_ref,
        })
    }

    async fn analyze_candidates(
        &self,
        request: &VisionExecutionRequest,
        input: &VisionInput,
        images: &[VisionModelImage],
        parent: Option<&OperationContext>,
    ) -> Result<(VisionModelResult, Vec<VisionPartition>, bool), VisionCapabilityError> {
        if request.auxiliary_models.is_empty() {
            return Err(VisionCapabilityError::AuxiliaryRouteUnavailable);
        }
        let task_context = self
            .load_task_context(request.capability.tenant_id, &request.task_context_refs)
            .await?;
        let mut last_failure = VisionCapabilityError::AuxiliaryRouteUnavailable;
        'candidate: for (ordinal, candidate) in request.auxiliary_models.iter().enumerate() {
            if parent.is_some_and(OperationContext::stopping) {
                return Err(VisionCapabilityError::ModelFailed);
            }
            let partitions = match candidate_partitions(candidate, images, input) {
                Ok(partitions) => partitions,
                Err(
                    error @ (VisionCapabilityError::InputLimit
                    | VisionCapabilityError::JointInputLimit),
                ) => {
                    last_failure = error;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let context = candidate_context(&request.model_invocation_context, candidate, ordinal)?;
            let degraded = partitions.len() > 1 && input.analysis_mode == VisionAnalysisMode::Joint;
            let mut results = Vec::with_capacity(partitions.len());
            for (index, partition) in partitions.iter().enumerate() {
                let model_request = VisionModelInvocation {
                    capture_provider_failure: request.auxiliary_models.len() > 1,
                    invocation_context: partition_invocation_context(&context, index)?,
                    model: candidate.clone(),
                    stream_budget: request.stream_budget,
                    images: images[partition.start..partition.start + partition.count].to_vec(),
                    question: input.question.clone(),
                    detail: input.detail,
                    analysis_mode: input.analysis_mode,
                    task_context: task_context.clone(),
                };
                let result = match parent {
                    Some(parent) => {
                        self.model
                            .analyze_observed(model_request, parent.clone())
                            .await
                    }
                    None => self.model.analyze(model_request).await,
                };
                match result {
                    Ok(result) => results.push(result),
                    Err(
                        error @ VisionCapabilityError::ModelFailure {
                            early_provider_cause: Some(cause),
                            code,
                            ..
                        },
                    ) if request.auxiliary_models.len() > 1
                        && cause.permits_alternate_provider()
                        && cause.runtime_code() == code =>
                    {
                        if !results.is_empty() {
                            return Err(VisionCapabilityError::CandidateSequenceStopped(code));
                        }
                        last_failure = error;
                        continue 'candidate;
                    }
                    // The unchanged fence remains the Run owner's decision. All
                    // other unqualified physical failures stop this Tool once.
                    Err(VisionCapabilityError::ModelFailure { code, .. })
                        if request.auxiliary_models.len() > 1
                            && code
                                != musteragent_runtime_contract::ErrorCode::AttemptLeaseStale =>
                    {
                        return Err(VisionCapabilityError::CandidateSequenceStopped(code));
                    }
                    Err(error) => return Err(error),
                }
            }
            return Ok((combine_model_results(results)?, partitions, degraded));
        }
        if request.auxiliary_models.len() > 1
            && let VisionCapabilityError::ModelFailure { code, .. } = last_failure
        {
            // All admitted providers have already been tried. Deliver exactly one
            // failure to the parent Tool, with no deferred model recovery loop.
            return Err(VisionCapabilityError::CandidateSequenceStopped(code));
        }
        Err(last_failure)
    }

    async fn resolve_images(
        &self,
        capability: &CapabilityInvocation,
        input: &VisionInput,
    ) -> Result<Vec<VisionModelImage>, VisionCapabilityError> {
        let mut resolved = Vec::with_capacity(input.images.len());
        let mut seen = std::collections::BTreeSet::new();
        for image in &input.images {
            let content_id = image
                .content_id
                .as_str()
                .parse::<ContentId>()
                .map_err(|_| VisionCapabilityError::InvalidInput)?;
            if !seen.insert(content_id) {
                return Err(VisionCapabilityError::InvalidInput);
            }
            let image_ref = if let Some(reference) = capability
                .authorized_content
                .iter()
                .find(|candidate| candidate.content_id == content_id)
                .cloned()
            {
                reference
            } else if let Some(authorization) = &self.artifact_content {
                authorization
                    .resolve_current_content(
                        capability.actor,
                        capability.conversation_id,
                        content_id,
                    )
                    .await
                    .map_err(|_| VisionCapabilityError::AuthorizationUnavailable)?
                    .ok_or(VisionCapabilityError::UnauthorizedContent)?
            } else {
                return Err(VisionCapabilityError::UnauthorizedContent);
            };
            if !media_type_is_image(&image_ref.media_type) {
                return Err(VisionCapabilityError::UnsupportedImageContent);
            }
            let bytes = self.content.read(capability.tenant_id, &image_ref).await?;
            verify_content(&image_ref, &bytes)
                .map_err(|_| VisionCapabilityError::UnsupportedImageContent)?;
            resolved.push(VisionModelImage {
                image_ref,
                label: image.label.clone(),
            });
        }
        Ok(resolved)
    }

    async fn load_task_context(
        &self,
        tenant_id: musteragent_identity::TenantId,
        references: &[ContentRef],
    ) -> Result<Vec<String>, VisionCapabilityError> {
        admission::load_task_context(self.content.as_ref(), tenant_id, references).await
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VisionInput {
    images: Vec<VisionInputImage>,
    question: String,
    #[serde(default)]
    detail: VisionDetail,
    #[serde(default)]
    analysis_mode: VisionAnalysisMode,
    #[serde(default)]
    allow_degraded_joint_context: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VisionInputImage {
    content_id: String,
    label: Option<String>,
}

impl VisionInput {
    fn validate(&self) -> Result<(), VisionCapabilityError> {
        if self.images.is_empty()
            || self.images.len() > MAX_LOGICAL_VISION_IMAGES
            || self.images.iter().any(|image| {
                image.content_id.is_empty()
                    || image.content_id.len() > 64
                    || image
                        .label
                        .as_ref()
                        .is_some_and(|label| label.trim().is_empty() || label.len() > 200)
            })
            || self.question.trim().is_empty()
            || self.question.len() > MAX_VISION_QUESTION_BYTES
        {
            Err(VisionCapabilityError::InvalidInput)
        } else {
            Ok(())
        }
    }
}

impl VisionModelResult {
    fn validate(&self) -> Result<(), VisionCapabilityError> {
        if self.analysis.trim().is_empty()
            || self.analysis.len() > MAX_VISION_ANALYSIS_BYTES
            || self.resolved_model_key.trim().is_empty()
            || self.resolved_model_key.len() > 200
        {
            Err(VisionCapabilityError::InvalidResult)
        } else {
            Ok(())
        }
    }
}

#[derive(Serialize)]
struct VisionOutput<'a> {
    analysis: &'a str,
    image_refs: Vec<VisionContentRef<'a>>,
    model_key: &'a str,
    mode: &'static str,
    batch: VisionBatchOutput<'a>,
}

#[derive(Debug, Clone, Serialize)]
struct VisionPartition {
    start: usize,
    count: usize,
}

#[derive(Serialize)]
struct VisionBatchOutput<'a> {
    logical_image_count: usize,
    provider_invocation_count: usize,
    partitions: &'a [VisionPartition],
    degraded_joint_context: bool,
}

#[derive(Serialize)]
struct VisionContentRef<'a> {
    content_id: String,
    media_type: &'a str,
    byte_length: u64,
    sha256: String,
}

impl<'a> From<&'a ContentRef> for VisionContentRef<'a> {
    fn from(value: &'a ContentRef) -> Self {
        Self {
            content_id: value.content_id.to_string(),
            media_type: value.media_type.as_str(),
            byte_length: value.byte_length,
            sha256: value.sha256.to_string(),
        }
    }
}

#[must_use]
pub fn model_supports_vision(model: &ModelRouteSnapshot) -> bool {
    // The same native-image rule the planner and provider mapper use.
    model.route.accepts_native_image()
}

#[must_use]
pub fn pinned_model_supports_vision(model: &PinnedModelRoute) -> bool {
    // The same native-image rule the planner and provider mapper use.
    model.route.accepts_native_image()
}

/// Resolves the exact physical primary/fallback route from the durable Agent
/// Loop ordinal without consulting mutable model configuration.
#[must_use]
pub fn active_pinned_model(
    model: &ModelRouteSnapshot,
    active_model_route_ordinal: u32,
) -> Option<PinnedModelRoute> {
    if active_model_route_ordinal == 0 {
        return Some(PinnedModelRoute {
            route: model.route.clone(),
            credential_grant_ref: model.credential_grant_ref.clone(),
        });
    }
    let index = usize::try_from(active_model_route_ordinal.checked_sub(1)?).ok()?;
    model.fallback_candidates.get(index).cloned()
}

fn vision_input_profile(
    model: &PinnedModelRoute,
) -> Result<VisionInputProfile, VisionCapabilityError> {
    VisionInputProfile::from_route(&model.route)
        .map_err(|_| VisionCapabilityError::RouteProfileInvalid)
}

fn validate_image_limits(
    images: &[VisionModelImage],
    profile: VisionInputProfile,
) -> Result<(), VisionCapabilityError> {
    if images.iter().any(|image| {
        image.image_ref.byte_length > profile.max_bytes_per_image
            || image.image_ref.byte_length > profile.max_total_image_bytes
    }) {
        Err(VisionCapabilityError::InputLimit)
    } else {
        Ok(())
    }
}

fn partition_images(
    images: &[VisionModelImage],
    profile: VisionInputProfile,
) -> Result<Vec<VisionPartition>, VisionCapabilityError> {
    if images.is_empty() || profile.max_images_per_request == 0 {
        return Err(VisionCapabilityError::InvalidInput);
    }
    let mut partitions = Vec::new();
    let mut start = 0;
    let mut count = 0;
    let mut bytes = 0_u64;
    for (index, image) in images.iter().enumerate() {
        let next_bytes = bytes
            .checked_add(image.image_ref.byte_length)
            .ok_or(VisionCapabilityError::InputLimit)?;
        if count > 0
            && (count == profile.max_images_per_request
                || next_bytes > profile.max_total_image_bytes)
        {
            partitions.push(VisionPartition { start, count });
            start = index;
            count = 0;
            bytes = 0;
        }
        bytes = bytes
            .checked_add(image.image_ref.byte_length)
            .ok_or(VisionCapabilityError::InputLimit)?;
        count += 1;
    }
    if count > 0 {
        partitions.push(VisionPartition { start, count });
    }
    Ok(partitions)
}

fn candidate_partitions(
    model: &PinnedModelRoute,
    images: &[VisionModelImage],
    input: &VisionInput,
) -> Result<Vec<VisionPartition>, VisionCapabilityError> {
    if !pinned_model_supports_vision(model) {
        return Err(VisionCapabilityError::AuxiliaryRouteInvalid);
    }
    let profile = vision_input_profile(model)?;
    validate_image_limits(images, profile)?;
    let partitions = partition_images(images, profile)?;
    if partitions.len() > 1
        && input.analysis_mode == VisionAnalysisMode::Joint
        && !input.allow_degraded_joint_context
    {
        return Err(VisionCapabilityError::JointInputLimit);
    }
    Ok(partitions)
}

fn candidate_context(
    base: &InvocationContext,
    model: &PinnedModelRoute,
    ordinal: usize,
) -> Result<InvocationContext, VisionCapabilityError> {
    if ordinal == 0 {
        return Ok(base.clone());
    }
    let mut context = base.clone();
    let name = format!(
        "vision-candidate:{ordinal}:{}",
        model.route.route_ref.as_str()
    );
    context.invocation_id = InvocationId::from_uuid(uuid::Uuid::new_v5(
        &base.invocation_id.into_uuid(),
        name.as_bytes(),
    ))
    .map_err(|_| VisionCapabilityError::IdentityMismatch)?;
    Ok(context)
}

fn partition_invocation_context(
    base: &InvocationContext,
    partition_index: usize,
) -> Result<InvocationContext, VisionCapabilityError> {
    if partition_index == 0 {
        return Ok(base.clone());
    }
    let name = format!("vision-partition:{partition_index}");
    let derived = uuid::Uuid::new_v5(&base.invocation_id.into_uuid(), name.as_bytes());
    let mut context = base.clone();
    context.invocation_id =
        InvocationId::from_uuid(derived).map_err(|_| VisionCapabilityError::ModelFailed)?;
    Ok(context)
}

fn combine_model_results(
    results: Vec<VisionModelResult>,
) -> Result<VisionModelResult, VisionCapabilityError> {
    let mut results = results.into_iter();
    let first = results.next().ok_or(VisionCapabilityError::InvalidResult)?;
    let mut analyses = vec![first.analysis];
    let resolved_model_key = first.resolved_model_key;
    let first_provider_ref = first.provider_request_ref;
    let mut multiple = false;
    for result in results {
        if result.resolved_model_key != resolved_model_key {
            return Err(VisionCapabilityError::InvalidResult);
        }
        multiple = true;
        analyses.push(result.analysis);
    }
    let analysis = if analyses.len() == 1 {
        analyses.pop().ok_or(VisionCapabilityError::InvalidResult)?
    } else {
        analyses
            .into_iter()
            .enumerate()
            .map(|(index, analysis)| format!("[Visual batch {}]\n{}", index + 1, analysis))
            .collect::<Vec<_>>()
            .join("\n\n")
    };
    Ok(VisionModelResult {
        analysis,
        resolved_model_key,
        provider_request_ref: if multiple { None } else { first_provider_ref },
    })
}

fn media_type_is_image(media_type: &MediaType) -> bool {
    media_type
        .as_str()
        .split(';')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .starts_with("image/")
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn validate_identity(request: &VisionExecutionRequest) -> Result<(), VisionCapabilityError> {
    let capability = &request.capability;
    let model = &request.model_invocation_context;
    use musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming;
    let timing_matches = match (&model.timing, &capability.timing) {
        (
            InvocationExecutionTiming::LegacyFixed { deadline: model },
            InvocationExecutionTiming::LegacyFixed { deadline: tool },
        ) => model <= tool,
        (
            InvocationExecutionTiming::ActivityV1(model),
            InvocationExecutionTiming::ActivityV1(tool),
        ) => model.business_deadline == tool.business_deadline,
        _ => false,
    };
    if capability.tool_key.logical_key() != "vision.analyze"
        || capability.capability_key.logical_key() != "vision.analyze"
        || capability.actor.tenant_id != capability.tenant_id
        || model.run_context.tenant_id != capability.tenant_id
        || model.run_context.conversation_id != capability.conversation_id
        || model.run_context.run_id != capability.run_id
        || model.attempt_context.attempt_id != capability.execution_attempt_id
        || model.attempt_context.attempt_number != capability.execution_attempt_number
        || !timing_matches
    {
        Err(VisionCapabilityError::IdentityMismatch)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error, Copy, Clone, Eq, PartialEq)]
pub enum VisionCapabilityError {
    #[error("vision Tool input is invalid")]
    InvalidInput,
    #[error("vision model result is invalid")]
    InvalidResult,
    #[error("vision capability identity differs from the active invocation")]
    IdentityMismatch,
    #[error("the requested image is not in the Run-authorized content set")]
    UnauthorizedContent,
    #[error("Artifact content authorization is unavailable")]
    AuthorizationUnavailable,
    #[error("the authorized content is not a supported image")]
    UnsupportedImageContent,
    #[error("the image batch exceeds the frozen visual input profile")]
    InputLimit,
    #[error("joint visual analysis cannot preserve same-request pixel context")]
    JointInputLimit,
    #[error("the current task context is corrupt or unsupported")]
    TaskContextInvalid,
    #[error("the Run has no pinned Admin auxiliary vision route")]
    AuxiliaryRouteUnavailable,
    #[error("the pinned Admin auxiliary vision route cannot process images")]
    AuxiliaryRouteInvalid,
    #[error("the active visual route has an invalid frozen input profile")]
    RouteProfileInvalid,
    #[error(transparent)]
    Content(#[from] ContentStoreError),
    #[error("visual provider sequence stopped on the original request")]
    CandidateSequenceStopped(musteragent_runtime_contract::ErrorCode),
    #[error("vision model invocation failed")]
    ModelFailed,
    #[error("vision model invocation failed: {code:?}, retryable={retryable}")]
    ModelFailure {
        early_provider_cause: Option<musteragent_runtime_model::ProviderErrorCode>,
        code: musteragent_runtime_contract::ErrorCode,
        retryable: bool,
        retry_after_ms: Option<u64>,
    },
}
