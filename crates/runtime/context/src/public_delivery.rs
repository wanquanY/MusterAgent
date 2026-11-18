//! Admission-sealed dialogue output semantics, applied before request encoding.
use crate::{ContextCodec, ContextMaterial, ContextPlanError};
use musteragent_content_ref::SchemaRef;
use musteragent_execution_context::{PublicDeliveryContract, RunOutputContract};
use musteragent_identity::VersionedKey;
use musteragent_model::ModelRoute;
use musteragent_runtime_model::JsonValue;

pub fn public_delivery_schema(
    codec: &dyn ContextCodec,
) -> Result<(SchemaRef, JsonValue), ContextPlanError> {
    public_delivery_schema_for(PublicDeliveryContract::V1, codec)
}

pub fn public_delivery_schema_for(
    contract: PublicDeliveryContract,
    codec: &dyn ContextCodec,
) -> Result<(SchemaRef, JsonValue), ContextPlanError> {
    Ok((
        SchemaRef {
            schema_key: VersionedKey::new(contract.key())
                .map_err(|_| ContextPlanError::invariant())?,
            schema_hash: contract.hash(),
        },
        codec.decode_json(contract.schema().as_bytes())?,
    ))
}

pub(crate) fn response_schema(
    material: &ContextMaterial,
    route: &ModelRoute,
    codec: &dyn ContextCodec,
) -> Result<Option<(SchemaRef, JsonValue)>, ContextPlanError> {
    let pins = material
        .runtime_snapshot
        .prompt
        .sections
        .iter()
        .filter(|p| p.section_key.as_str() == "dialogue.output-contract")
        .collect::<Vec<_>>();
    if pins.is_empty() {
        return Ok(None);
    }
    if pins.len() != 1 {
        return Err(ContextPlanError::invariant());
    }
    let contract = RunOutputContract::from_prompt_ref(&pins[0].content_ref)
        .ok_or_else(ContextPlanError::invariant)?;
    let expected = contract.prompt();
    if !material
        .prompt_sections
        .iter()
        .any(|s| s.content_ref == pins[0].content_ref && s.bytes == expected.as_bytes())
    {
        return Err(ContextPlanError::invariant());
    }
    let Some(response_contract) = contract.response_contract() else {
        // No output-text parser or provider JSON response format participates in
        // native publication. The signed route still must support native tools.
        if !route.capabilities.iter().any(|c| c.as_str() == "tools") {
            return Err(ContextPlanError {
                early_provider_cause: None,
                code: musteragent_runtime_contract::ErrorCode::CapabilityUnavailable,
                retryable: false,
                retry_after_ms: None,
            });
        }
        return Ok(None);
    };
    // Historical runs keep their exact signed response-schema requirement.
    if !route
        .capabilities
        .iter()
        .any(|c| c.as_str() == "json_schema_output")
    {
        return Err(ContextPlanError {
            early_provider_cause: None,
            code: musteragent_runtime_contract::ErrorCode::CapabilityUnavailable,
            retryable: false,
            retry_after_ms: None,
        });
    }
    public_delivery_schema_for(response_contract, codec).map(Some)
}
