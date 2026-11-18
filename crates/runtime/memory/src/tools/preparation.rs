//! Host-private proposal preparation journal in the original Content Store.
//! The package sees business input only. These persisted identity/time fields
//! are produced by the Host and never constitute consent or commit authority.
use super::*;
use musteragent_content_ref::ContentRef;
use musteragent_identity::ContentId;
use musteragent_memory_business::preparation::{WritePlan, WritePreparation};
use musteragent_runtime_capability::OperationContext;
use musteragent_runtime_content::{ContentStoreError, verify_content};

const MEDIA: &str = "application/vnd.musteragent.memory-prepared-proposal+json";
const MAX_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Binding {
    tenant_id: Uuid,
    principal_id: Uuid,
    agent_id: Uuid,
    conversation_id: Uuid,
    run_id: Uuid,
    invocation_id: Uuid,
    requested_by_attempt_id: Uuid,
    tool_call_id: String,
    idempotency_key: String,
    tool_key: String,
    capability_key: String,
    provider_id: String,
    evidence_record_id: Uuid,
    approval_interaction_id: Option<Uuid>,
    request_id: Uuid,
    request_media_type: String,
    request_length: u64,
    request_hash: String,
    request_schema_key: String,
    request_schema_hash: String,
    result_schema_key: String,
    result_schema_hash: String,
    runtime_snapshot_id: Uuid,
}

impl Binding {
    pub(super) fn from_context(context: &MemoryToolContext) -> Result<Self, MemoryToolError> {
        validate_context(context)?;
        let invocation = &context.invocation;
        if context.actor != invocation.actor {
            return Err(MemoryToolError::IdentityMismatch);
        }
        Ok(Self {
            tenant_id: context.actor.tenant_id.into_uuid(),
            principal_id: context.actor.principal_id.into_uuid(),
            agent_id: context.agent_id.into_uuid(),
            conversation_id: invocation.conversation_id.into_uuid(),
            run_id: invocation.run_id.into_uuid(),
            invocation_id: invocation.invocation_id.into_uuid(),
            requested_by_attempt_id: invocation.requested_by_attempt_id.into_uuid(),
            tool_call_id: invocation.tool_call_id.as_str().into(),
            idempotency_key: invocation.idempotency_key.as_str().into(),
            tool_key: invocation.tool_key.as_str().into(),
            capability_key: invocation.capability_key.as_str().into(),
            provider_id: invocation.capability_provider_id.to_string(),
            evidence_record_id: context.evidence_record_id.into_uuid(),
            approval_interaction_id: context.approval_interaction_id.map(|id| id.into_uuid()),
            request_id: invocation.request_ref.content_id.into_uuid(),
            request_media_type: invocation.request_ref.media_type.as_str().into(),
            request_length: invocation.request_ref.byte_length,
            request_hash: invocation.request_ref.sha256.to_string(),
            request_schema_key: invocation.request_schema.schema_key.as_str().into(),
            request_schema_hash: invocation.request_schema.schema_hash.to_string(),
            result_schema_key: invocation.result_schema.schema_key.as_str().into(),
            result_schema_hash: invocation.result_schema.schema_hash.to_string(),
            runtime_snapshot_id: context.run_context.runtime_snapshot_id.into_uuid(),
        })
    }
}

#[derive(PartialEq, Serialize, Deserialize)]
enum Schema {
    #[serde(rename = "musteragent.memory.prepared-proposal.v1")]
    V1,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedProposal {
    schema: Schema,
    implementation: String,
    binding: Binding,
    prepared_at_ms: i64,
    business: WritePreparation,
}

pub(super) struct ProposalDecision {
    pub input: proposal::ProposalInput,
    pub prepared_at: UtcTimestamp,
}

pub(super) async fn proposal_decision(
    content: &dyn ContentStore,
    planner: &dyn crate::MemoryWritePlanner,
    context: &MemoryToolContext,
    request: &[u8],
    execution: Option<&OperationContext>,
) -> Result<ProposalDecision, MemoryToolError> {
    verify_content(&context.invocation.request_ref, request)
        .map_err(|_| MemoryToolError::InvalidResult)?;
    let binding = Binding::from_context(context)?;
    let implementation = planner.identity();
    let id = identity(context.invocation.invocation_id, "proposal-plan");
    let prepared = match read_prepared(
        content,
        context.actor.tenant_id,
        id,
        &binding,
        implementation.as_str(),
        request,
        execution,
    )
    .await?
    {
        Some(prepared) => prepared,
        None => {
            let business =
                crate::write_planner::prepare(planner, &binding.tool_key, request, execution)
                    .await?;
            if !matches!(
                business.plan,
                WritePlan::ProposeV2(_) | WritePlan::ProposeV3(_)
            ) {
                return Err(MemoryToolError::UnsupportedTool);
            }
            let prepared = PreparedProposal {
                schema: Schema::V1,
                implementation: implementation.as_str().into(),
                binding: binding.clone(),
                prepared_at_ms: context.now.unix_millis(),
                business,
            };
            let bytes = encode(&prepared)?;
            ensure_running(execution)?;
            match put_exact(content, context.actor.tenant_id, id, MEDIA, bytes).await {
                Ok(_) => prepared,
                Err(
                    error @ (ContentStoreError::Unavailable | ContentStoreError::IdentityConflict),
                ) => {
                    // A lost acknowledgement or concurrent attempt may have
                    // persisted first. Recover that exact plan, never overwrite
                    // it or reselect a newer business decision.
                    read_prepared(
                        content,
                        context.actor.tenant_id,
                        id,
                        &binding,
                        implementation.as_str(),
                        request,
                        execution,
                    )
                    .await?
                    .ok_or_else(|| content_error(error))?
                }
                Err(error) => return Err(content_error(error)),
            }
        }
    };
    if prepared.prepared_at_ms > context.now.unix_millis() {
        return Err(MemoryToolError::Store);
    }
    let input = match prepared.business.plan {
        WritePlan::ProposeV2(input) | WritePlan::ProposeV3(input) => input,
        _ => return Err(MemoryToolError::InvalidResult),
    };
    Ok(ProposalDecision {
        input,
        prepared_at: UtcTimestamp::from_unix_millis(prepared.prepared_at_ms),
    })
}

async fn read_prepared(
    content: &dyn ContentStore,
    tenant: TenantId,
    id: ContentId,
    binding: &Binding,
    implementation: &str,
    request: &[u8],
    execution: Option<&OperationContext>,
) -> Result<Option<PreparedProposal>, MemoryToolError> {
    let Some(reference) =
        crate::retrieval::read_stage(execution, content.find_prepared_at(tenant, id))
            .await
            .map_err(map_retrieval_error)?
            .map_err(content_error)?
    else {
        return Ok(None);
    };
    if reference.content_id != id
        || reference.media_type.as_str() != MEDIA
        || reference.byte_length > MAX_BYTES as u64
    {
        return Err(MemoryToolError::InvalidResult);
    }
    let bytes = crate::retrieval::read_stage(execution, content.read(tenant, &reference))
        .await
        .map_err(map_retrieval_error)?
        .map_err(content_error)?;
    verify_content(&reference, &bytes).map_err(|_| MemoryToolError::InvalidResult)?;
    let prepared: PreparedProposal =
        serde_json::from_slice(&bytes).map_err(|_| MemoryToolError::InvalidResult)?;
    if prepared.binding != *binding
        || prepared.implementation != implementation
        || encode(&prepared)? != bytes
    {
        return Err(MemoryToolError::InvalidResult);
    }
    WritePreparation::decode_for_request(
        &binding.tool_key,
        request,
        &prepared
            .business
            .canonical_bytes()
            .map_err(|_| MemoryToolError::InvalidResult)?,
    )
    .map_err(|_| MemoryToolError::InvalidResult)?;
    Ok(Some(prepared))
}

fn encode(prepared: &PreparedProposal) -> Result<Vec<u8>, MemoryToolError> {
    let bytes = serde_json::to_vec(prepared).map_err(|_| MemoryToolError::InvalidResult)?;
    if bytes.len() > MAX_BYTES {
        return Err(MemoryToolError::InvalidResult);
    }
    Ok(bytes)
}

pub(super) fn identity(invocation: musteragent_identity::InvocationId, purpose: &str) -> ContentId {
    ContentId::from_uuid(Uuid::new_v5(
        &invocation.into_uuid(),
        format!("memory-private-content@1:{purpose}").as_bytes(),
    ))
    .expect("versioned non-nil content identity")
}

pub(super) async fn put_exact(
    content: &dyn ContentStore,
    tenant: TenantId,
    id: ContentId,
    media: &str,
    bytes: Vec<u8>,
) -> Result<ContentRef, ContentStoreError> {
    let expected = ContentRef {
        content_id: id,
        media_type: MediaType::new(media).map_err(|_| ContentStoreError::Invariant)?,
        byte_length: bytes.len() as u64,
        sha256: Sha256::digest(&bytes),
    };
    let reference = content
        .put_at(
            id,
            PutContent {
                tenant_id: tenant,
                media_type: expected.media_type.clone(),
                bytes,
            },
        )
        .await?;
    if reference != expected {
        return Err(ContentStoreError::Invariant);
    }
    Ok(reference)
}

pub(super) fn content_error(error: ContentStoreError) -> MemoryToolError {
    match error {
        ContentStoreError::Unavailable => MemoryToolError::Content,
        _ => MemoryToolError::InvalidResult,
    }
}

pub(super) async fn prepare_proposal(
    content: &dyn ContentStore,
    planner: &dyn crate::MemoryWritePlanner,
    context: MemoryToolContext,
    bytes: &[u8],
    execution: Option<&OperationContext>,
) -> Result<MemoryToolPreparation, MemoryToolError> {
    let decision = proposal_decision(content, planner, &context, bytes, execution).await?;
    ensure_running(execution)?;
    let input = decision.input;
    let proposal_id = proposal_id(context.invocation.invocation_id)?;
    let claim = ExperienceClaimDocument::manual(
        input.claim.clone(),
        input.conditions.clone(),
        input.outcome.clone(),
        input.sensitivity.clone(),
    )
    .encode()
    .map_err(|_| MemoryToolError::InvalidResult)?;
    let claim_ref = put_exact(
        content,
        context.actor.tenant_id,
        identity(context.invocation.invocation_id, "proposal-claim"),
        EXPERIENCE_CLAIM_MEDIA_TYPE,
        claim,
    )
    .await
    .map_err(content_error)?;
    ensure_running(execution)?;
    let confidence_bps = confidence_bps(input.confidence)?;
    let target_scope = match input.scope.as_str() {
        "user" => EvolutionScope::User {
            principal_id: context.actor.principal_id,
        },
        "agent_profile" => EvolutionScope::Agent {
            agent_id: context.agent_id,
        },
        _ => return Err(MemoryToolError::InvalidInput),
    };
    let candidate = ExperienceCandidate {
        proposal_id,
        tenant_id: context.actor.tenant_id,
        proposed_by: context.actor.principal_id,
        origin: musteragent_evolution::ExperienceCandidateOrigin::RuntimeTool,
        source_draft_id: None,
        target_scope,
        target_memory_id: None,
        supersedes_proposal_id: input
            .supersedes_candidate_id
            .map(|value| {
                EvolutionProposalId::from_uuid(value).map_err(|_| MemoryToolError::InvalidInput)
            })
            .transpose()?,
        kind: experience_kind(&input.kind)?,
        claim_ref,
        source_run_ids: BTreeSet::from([context.run_context.run_id]),
        evidence_record_ids: BTreeSet::from([context.evidence_record_id]),
        status: CandidateStatus::Candidate,
        confidence: Some(confidence_bps),
        created_at: decision.prepared_at,
    };
    candidate
        .validate()
        .map_err(|_| MemoryToolError::InvalidInput)?;
    let output = serde_json::to_vec(&ProposalOutput {
        candidate_id: proposal_id.to_string(),
        status: "candidate",
        published: false,
    })
    .map_err(|_| MemoryToolError::InvalidResult)?;
    let result_ref = put_exact(
        content,
        context.actor.tenant_id,
        identity(context.invocation.invocation_id, "proposal-result"),
        "application/json",
        output,
    )
    .await
    .map_err(content_error)?;
    ensure_running(execution)?;
    let result = CapabilityResult {
        result_hash: result_ref.sha256,
        result_ref,
        result_schema: context.invocation.result_schema.clone(),
        provider_request_ref: None,
    };
    Ok(MemoryToolPreparation::Commit {
        result,
        mutation: Box::new(MemoryToolMutation::Propose(Box::new(candidate))),
    })
}

pub(super) fn ensure_running(execution: Option<&OperationContext>) -> Result<(), MemoryToolError> {
    if execution.is_some_and(OperationContext::stopping) {
        Err(MemoryToolError::Execution(
            musteragent_runtime_contract::ErrorCode::RunTransitionInvalid,
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
