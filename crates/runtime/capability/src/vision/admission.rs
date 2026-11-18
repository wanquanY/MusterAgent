//! The Home verifies exactly the request prepared by the original Vision tool.
//! Partitioning, limits and model evidence reuse the production implementation.
use super::*;
use musteragent_runtime_model::{AdmittedModelInvocation, MODEL_REQUEST_MEDIA_TYPE};

pub struct VisionAdmissionBasis<'a> {
    pub context: &'a InvocationContext,
    pub model: &'a PinnedModelRoute,
    pub earlier_models: &'a [&'a PinnedModelRoute],
    pub stream_budget: ModelStreamBudgetSnapshot,
    pub arguments: &'a serde_json::Value,
    pub authorized_content: &'a [ContentRef],
    pub task_context_refs: &'a [ContentRef],
}

pub async fn validate_vision_admission(
    content: &dyn ContentStore,
    basis: VisionAdmissionBasis<'_>,
    admission: &AdmittedModelInvocation,
) -> Result<Vec<(InvocationId, musteragent_model::SignedModelRouteHash)>, VisionCapabilityError> {
    let invocation = &admission.invocation;
    admission
        .validate()
        .map_err(|_| VisionCapabilityError::InvalidInput)?;
    if admission.route != basis.model.route
        || invocation.credential_grant_ref != basis.model.credential_grant_ref
        || invocation.request_ref.media_type.as_str() != MODEL_REQUEST_MEDIA_TYPE
        || invocation.tenant_id != basis.context.run_context.tenant_id
        || invocation.conversation_id != basis.context.run_context.conversation_id
        || invocation.run_id != basis.context.run_context.run_id
        || invocation.attempt_id != basis.context.attempt_context.attempt_id
        || invocation.idempotency_key.as_str() != format!("vision:{}", invocation.invocation_id)
    {
        return Err(VisionCapabilityError::InvalidInput);
    }
    let input: VisionInput = serde_json::from_value(basis.arguments.clone())
        .map_err(|_| VisionCapabilityError::InvalidInput)?;
    input.validate()?;
    let mut images = Vec::with_capacity(input.images.len());
    let mut seen = std::collections::BTreeSet::new();
    for image in &input.images {
        let id = image
            .content_id
            .parse::<ContentId>()
            .map_err(|_| VisionCapabilityError::InvalidInput)?;
        if !seen.insert(id) {
            return Err(VisionCapabilityError::InvalidInput);
        }
        let reference = basis
            .authorized_content
            .iter()
            .find(|v| v.content_id == id)
            .filter(|v| media_type_is_image(&v.media_type))
            .ok_or(VisionCapabilityError::UnauthorizedContent)?;
        images.push(VisionModelImage {
            image_ref: reference.clone(),
            label: image.label.clone(),
        });
    }
    let mut predecessors = Vec::new();
    for (ordinal, previous) in basis.earlier_models.iter().enumerate() {
        match candidate_partitions(previous, &images, &input) {
            Ok(_) => {
                let previous_context = candidate_context(basis.context, previous, ordinal)?;
                let hash = musteragent_model::SignedModelRouteHash::digest(&previous.route)
                    .map_err(|_| VisionCapabilityError::AuxiliaryRouteInvalid)?;
                predecessors.push((previous_context.invocation_id, hash));
            }
            Err(VisionCapabilityError::InputLimit | VisionCapabilityError::JointInputLimit) => {}
            Err(error) => return Err(error),
        }
    }
    let partitions = candidate_partitions(basis.model, &images, &input)?;
    let candidate = candidate_context(basis.context, basis.model, basis.earlier_models.len())?;
    let mut selected = None;
    for (index, partition) in partitions.iter().enumerate() {
        let context = partition_invocation_context(&candidate, index)?;
        if context.invocation_id == invocation.invocation_id {
            selected = Some((context, partition));
            break;
        }
    }
    let (context, partition) = selected.ok_or(VisionCapabilityError::InvalidInput)?;
    let task_context =
        load_task_context(content, invocation.tenant_id, basis.task_context_refs).await?;
    let request = VisionModelInvocation {
        capture_provider_failure: false,
        invocation_context: context,
        model: basis.model.clone(),
        stream_budget: basis.stream_budget,
        images: images[partition.start..partition.start + partition.count].to_vec(),
        question: input.question,
        detail: input.detail,
        analysis_mode: input.analysis_mode,
        task_context,
    };
    let canonical = crate::vision_model::canonical_request(&request)?;
    let expected = crate::vision_model::model_request_evidence(&request, &canonical)?;
    let bytes = content
        .read(invocation.tenant_id, &invocation.request_ref)
        .await?;
    verify_content(&invocation.request_ref, &bytes)
        .map_err(|_| VisionCapabilityError::InvalidInput)?;
    if bytes != expected {
        return Err(VisionCapabilityError::InvalidInput);
    }
    Ok(predecessors)
}

pub(super) async fn load_task_context(
    content: &dyn ContentStore,
    tenant_id: musteragent_identity::TenantId,
    references: &[ContentRef],
) -> Result<Vec<String>, VisionCapabilityError> {
    let mut remaining = MAX_TASK_CONTEXT_BYTES;
    let mut context = Vec::new();
    for reference in references {
        if remaining == 0 {
            break;
        }
        let bytes = content.read(tenant_id, reference).await?;
        verify_content(reference, &bytes).map_err(|_| VisionCapabilityError::TaskContextInvalid)?;
        let value =
            std::str::from_utf8(&bytes).map_err(|_| VisionCapabilityError::TaskContextInvalid)?;
        if value.trim().is_empty() {
            continue;
        }
        let boundary = floor_char_boundary(value, remaining.min(value.len()));
        context.push(value[..boundary].to_owned());
        remaining = remaining.saturating_sub(boundary);
    }
    Ok(context)
}
