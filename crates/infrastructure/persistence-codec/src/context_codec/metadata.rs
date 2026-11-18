//! The existing model-request identity mappings, reused without fabricating a
//! model invocation. Decoded metadata is never an execution authority.
use super::*;
const MAX_METADATA_BYTES: usize = 16 * 1024;

pub fn encode_attempt(value: &AttemptContext) -> Result<Vec<u8>, ContextPlanError> {
    encode(attempt_context(value))
}
pub fn decode_attempt(bytes: &[u8]) -> Result<AttemptContext, ContextPlanError> {
    let value = model_decode::decode_attempt_metadata(&decode(bytes)?)?;
    if encode_attempt(&value)? != bytes {
        return Err(ContextPlanError::invariant());
    }
    Ok(value)
}
pub fn encode_run(value: &RunContext) -> Result<Vec<u8>, ContextPlanError> {
    encode(run_context(value))
}
pub fn decode_run(bytes: &[u8]) -> Result<RunContext, ContextPlanError> {
    let value = model_decode::decode_run_metadata(&decode(bytes)?)?;
    if encode_run(&value)? != bytes {
        return Err(ContextPlanError::invariant());
    }
    Ok(value)
}
fn encode(value: Value) -> Result<Vec<u8>, ContextPlanError> {
    let bytes = serde_json::to_vec(&value).map_err(|_| ContextPlanError::invariant())?;
    if bytes.len() > MAX_METADATA_BYTES {
        return Err(ContextPlanError::invariant());
    }
    Ok(bytes)
}
fn decode(bytes: &[u8]) -> Result<Value, ContextPlanError> {
    if bytes.is_empty() || bytes.len() > MAX_METADATA_BYTES {
        return Err(ContextPlanError::invariant());
    }
    serde_json::from_slice(bytes).map_err(|_| ContextPlanError::invariant())
}

pub fn encode_invocation(value: &InvocationContext) -> Result<Vec<u8>, ContextPlanError> {
    encode(invocation(value))
}
pub fn decode_invocation(bytes: &[u8]) -> Result<InvocationContext, ContextPlanError> {
    let value = model_decode::decode_invocation_metadata(&decode(bytes)?)?;
    if encode_invocation(&value)? != bytes {
        return Err(ContextPlanError::invariant());
    }
    Ok(value)
}
pub fn encode_route(value: &musteragent_model::ModelRoute) -> Result<Vec<u8>, ContextPlanError> {
    value
        .validate()
        .map_err(|_| ContextPlanError::invariant())?;
    encode(route(value))
}
pub fn decode_route(bytes: &[u8]) -> Result<musteragent_model::ModelRoute, ContextPlanError> {
    let value = model_decode::decode_route_metadata(&decode(bytes)?)?;
    if encode_route(&value)? != bytes {
        return Err(ContextPlanError::invariant());
    }
    Ok(value)
}
