//! Exact reconstruction of versioned canonical model request content. Decoding
//! validates structure only: it does not authorize its identities or grant refs.
mod content;
mod identity;
#[cfg(test)]
mod tests;
use super::*;
use musteragent_runtime_contract::OpaqueExternalRef;
use musteragent_runtime_model::{ContinuationRef, ModelInvocationRouteRole, ModelText};
use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    str::FromStr,
};

type Result<T> = std::result::Result<T, ContextPlanError>;
type Object = Map<String, Value>;

impl CanonicalJsonContextCodec {
    /// Reconstruct only the format named by the original content digest. A
    /// newly introduced model name can never be erased to fit legacy bytes.
    #[cfg(test)]
    pub(crate) fn encode_pinned_model_request(
        &self,
        request: &CanonicalModelRequest,
        reference: &musteragent_content_ref::ContentRef,
    ) -> Result<(super::ModelRequestSchema, Vec<u8>)> {
        checked(request.validate())?;
        for schema in [
            super::ModelRequestSchema::NativePublicationV8,
            super::ModelRequestSchema::ApprovalObservationV7,
            super::ModelRequestSchema::DialogueInputV6,
            super::ModelRequestSchema::DeliveryRepairV5,
            super::ModelRequestSchema::ActivityV4,
            super::ModelRequestSchema::V3,
            super::ModelRequestSchema::LegacyV2,
        ] {
            if !schema_matches_request(schema, request) {
                continue;
            }
            if schema == super::ModelRequestSchema::LegacyV2
                && request
                    .tools
                    .iter()
                    .any(|tool| tool.model_name != tool.name)
            {
                continue;
            }
            let bytes = checked(serde_json::to_vec(&super::model_request_for_schema(
                request, schema,
            )))?;
            if reference.byte_length == bytes.len() as u64
                && reference.sha256 == musteragent_content_ref::Sha256::digest(&bytes)
            {
                return Ok((schema, bytes));
            }
        }
        Err(ContextPlanError::invariant())
    }

    /// `maximum_bytes` comes from the caller's fixed content/transport budget,
    /// never from a field inside this document. Historical bytes are not
    /// normalized or silently rebound to a different routing/Worker identity.
    pub fn decode_model_request(
        &self,
        bytes: &[u8],
        maximum_bytes: NonZeroUsize,
    ) -> Result<CanonicalModelRequest> {
        self.decode_versioned_model_request(bytes, maximum_bytes)
            .map(|(_, request)| request)
    }

    pub(crate) fn decode_versioned_model_request(
        &self,
        bytes: &[u8],
        maximum_bytes: NonZeroUsize,
    ) -> Result<(super::ModelRequestSchema, CanonicalModelRequest)> {
        if bytes.is_empty() || bytes.len() > maximum_bytes.get() {
            return Err(ContextPlanError::invariant());
        }
        let value: Value = checked(serde_json::from_slice(bytes))?;
        let object = object(&value)?;
        let schema = match required_str(object, "schema_version")? {
            "musteragent.canonical-model-request.v2" => super::ModelRequestSchema::LegacyV2,
            "musteragent.canonical-model-request.v3" => super::ModelRequestSchema::V3,
            "musteragent.canonical-model-request.v4" => super::ModelRequestSchema::ActivityV4,
            "musteragent.canonical-model-request.v5" => super::ModelRequestSchema::DeliveryRepairV5,
            "musteragent.canonical-model-request.v6" => super::ModelRequestSchema::DialogueInputV6,
            "musteragent.canonical-model-request.v7" => {
                super::ModelRequestSchema::ApprovalObservationV7
            }
            "musteragent.canonical-model-request.v8" => {
                super::ModelRequestSchema::NativePublicationV8
            }
            _ => return Err(ContextPlanError::invariant()),
        };
        let budget = object_field(object, "stream_budget")?;
        let request = CanonicalModelRequest {
            invocation: identity::invocation(field(object, "invocation")?)?,
            route: identity::route(field(object, "route")?)?,
            route_role: route_role(required_str(object, "route_role")?)?,
            credential_grant_ref: checked(musteragent_model::CredentialGrantRef::new(
                required_str(object, "credential_grant_ref")?,
            ))?,
            continuation_ref: optional(object, "continuation_ref", continuation)?,
            instructions: list(object, "instructions", text)?,
            input: list(object, "input", content::message)?,
            runtime_events: list(object, "runtime_events", content::runtime_event)?,
            tools: list(object, "tools", |value| content::tool(value, schema))?,
            response_schema: optional(object, "response_schema", |v| {
                let o = self::object(v)?;
                Ok((
                    content::schema_ref(field(o, "schema_ref")?)?,
                    json_from_serde(field(o, "schema")?.clone())?,
                ))
            })?,
            reasoning_policy: match required_str(object, "reasoning_policy")? {
                "disabled" => ReasoningPolicy::Disabled,
                "summary" => ReasoningPolicy::Summary,
                "full" => ReasoningPolicy::Full,
                _ => return Err(ContextPlanError::invariant()),
            },
            cache_policy: match required_str(object, "cache_policy")? {
                "provider_default" => CachePolicy::ProviderDefault,
                "explicit_ephemeral_prefix" => CachePolicy::ExplicitEphemeralPrefix,
                "disabled" => CachePolicy::Disabled,
                _ => return Err(ContextPlanError::invariant()),
            },
            stream_budget: musteragent_runtime_snapshot::ModelStreamBudgetSnapshot {
                max_event_bytes: positive_usize(budget, "max_event_bytes")?,
                max_events: positive_u32(budget, "max_events")?,
                max_total_bytes: positive_u64(budget, "max_total_bytes")?,
                max_tool_calls: positive_u32(budget, "max_tool_calls")?,
                max_tool_argument_bytes: positive_usize(budget, "max_tool_argument_bytes")?,
                max_reasoning_replay_bytes: positive_usize(budget, "max_reasoning_replay_bytes")?,
            },
            max_output_tokens: positive_u64(object, "max_output_tokens")?,
            temperature: optional(object, "temperature", |v| {
                v.as_f64()
                    .filter(|v| v.is_finite())
                    .ok_or_else(ContextPlanError::invariant)
            })?,
        };
        if super::has_native_publication_event(&request)
            || super::has_delivery_repair_event(&request)
            || super::has_dialogue_input_event(&request)
            || super::has_approval_observation(&request)
        {
            checked(request.validate())?;
        }
        if !schema_matches_request(schema, &request) {
            return Err(ContextPlanError::invariant());
        }
        // The existing encoder validates the domain model. Exact equality also
        // closes unknown/missing/duplicate fields, aliases and noncanonical JSON
        // at every nested object without a second hand-maintained key allowlist.
        if checked(serde_json::to_vec(&super::model_request_for_schema(
            &request, schema,
        )))? != bytes
        {
            return Err(ContextPlanError::invariant());
        }
        Ok((schema, request))
    }
}
fn checked<T, E>(result: std::result::Result<T, E>) -> Result<T> {
    result.map_err(|_| ContextPlanError::invariant())
}
fn object(value: &Value) -> Result<&Object> {
    value.as_object().ok_or_else(ContextPlanError::invariant)
}
fn field<'a>(object: &'a Object, key: &str) -> Result<&'a Value> {
    object.get(key).ok_or_else(ContextPlanError::invariant)
}
fn object_field<'a>(o: &'a Object, key: &str) -> Result<&'a Object> {
    object(field(o, key)?)
}
fn string(value: &Value) -> Result<&str> {
    value.as_str().ok_or_else(ContextPlanError::invariant)
}
fn parse<T: FromStr>(value: &Value) -> Result<T> {
    checked(string(value)?.parse())
}
fn id<T: FromStr>(o: &Object, key: &str) -> Result<T> {
    parse(field(o, key)?)
}
fn optional<T>(
    o: &Object,
    key: &str,
    decode: impl FnOnce(&Value) -> Result<T>,
) -> Result<Option<T>> {
    let v = field(o, key)?;
    if v.is_null() {
        Ok(None)
    } else {
        decode(v).map(Some)
    }
}
fn list<T>(o: &Object, key: &str, decode: impl Fn(&Value) -> Result<T>) -> Result<Vec<T>> {
    field(o, key)?
        .as_array()
        .ok_or_else(ContextPlanError::invariant)?
        .iter()
        .map(decode)
        .collect()
}
fn boolean(o: &Object, key: &str) -> Result<bool> {
    field(o, key)?
        .as_bool()
        .ok_or_else(ContextPlanError::invariant)
}
fn positive_u64(o: &Object, key: &str) -> Result<NonZeroU64> {
    NonZeroU64::new(required_u64(o, key)?).ok_or_else(ContextPlanError::invariant)
}
fn positive_u32(o: &Object, key: &str) -> Result<NonZeroU32> {
    NonZeroU32::new(checked(u32::try_from(required_u64(o, key)?))?)
        .ok_or_else(ContextPlanError::invariant)
}
fn positive_usize(o: &Object, key: &str) -> Result<NonZeroUsize> {
    NonZeroUsize::new(checked(usize::try_from(required_u64(o, key)?))?)
        .ok_or_else(ContextPlanError::invariant)
}
fn text(v: &Value) -> Result<ModelText> {
    checked(ModelText::new(string(v)?))
}
fn continuation(v: &Value) -> Result<ContinuationRef> {
    Ok(ContinuationRef::from_opaque(checked(
        OpaqueExternalRef::new(string(v)?),
    )?))
}
fn route_role(value: &str) -> Result<ModelInvocationRouteRole> {
    if value == "chat" {
        return Ok(ModelInvocationRouteRole::Chat);
    }
    (1..=7)
        .filter_map(|v| musteragent_model::AuxiliaryModelRole::from_canonical_code(v).ok())
        .find(|role| role.as_str() == value)
        .map(ModelInvocationRouteRole::Auxiliary)
        .ok_or_else(ContextPlanError::invariant)
}

fn key(v: &Value) -> Result<musteragent_identity::VersionedKey> {
    checked(musteragent_identity::VersionedKey::new(string(v)?))
}

pub(super) fn decode_run_metadata(value: &Value) -> Result<RunContext> {
    identity::run_context(value)
}
pub(super) fn decode_attempt_metadata(value: &Value) -> Result<AttemptContext> {
    identity::attempt_context(value)
}

pub(super) fn decode_invocation_metadata(value: &Value) -> Result<InvocationContext> {
    identity::invocation(value)
}
pub(super) fn decode_route_metadata(value: &Value) -> Result<musteragent_model::ModelRoute> {
    identity::route(value)
}

fn schema_matches_request(
    schema: super::ModelRequestSchema,
    request: &CanonicalModelRequest,
) -> bool {
    let native = super::has_native_publication_event(request);
    if native || schema == super::ModelRequestSchema::NativePublicationV8 {
        return native && schema == super::ModelRequestSchema::NativePublicationV8;
    }
    let approval = super::has_approval_observation(request);
    if approval || schema == super::ModelRequestSchema::ApprovalObservationV7 {
        return approval && schema == super::ModelRequestSchema::ApprovalObservationV7;
    }
    let dialogue = super::has_dialogue_input_event(request);
    if dialogue || schema == super::ModelRequestSchema::DialogueInputV6 {
        return dialogue && schema == super::ModelRequestSchema::DialogueInputV6;
    }
    let repair = super::has_delivery_repair_event(request);
    if repair || schema == super::ModelRequestSchema::DeliveryRepairV5 {
        return repair && schema == super::ModelRequestSchema::DeliveryRepairV5;
    }
    (schema == super::ModelRequestSchema::ActivityV4)
        == request.invocation.timing.legacy_deadline().is_none()
}
