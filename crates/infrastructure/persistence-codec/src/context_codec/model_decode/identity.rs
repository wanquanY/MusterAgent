use super::*;
use musteragent_identity::{AttemptNumber, FencingToken, HomeEpoch, UtcTimestamp};
use musteragent_model::{ApiFormat, ModelRoute, PricingVersionRef};

pub(super) fn invocation(v: &Value) -> Result<InvocationContext> {
    let o = object(v)?;
    Ok(InvocationContext {
        invocation_id: id(o, "invocation_id")?,
        run_context: run_context(field(o, "run_context")?)?,
        attempt_context: attempt_context(field(o, "attempt_context")?)?,
        parent_invocation_id: optional(o, "parent_invocation_id", parse)?,
        causation_id: id(o, "causation_id")?,
        correlation_id: id(o, "correlation_id")?,
        timing: super::super::timing::decode(o)?,
    })
}
fn timestamp(o: &Object, key: &str) -> Result<UtcTimestamp> {
    Ok(UtcTimestamp::from_unix_millis(
        field(o, key)?
            .as_i64()
            .ok_or_else(ContextPlanError::invariant)?,
    ))
}
pub(super) fn run_context(v: &Value) -> Result<RunContext> {
    let o = object(v)?;
    let activity_kind = match required_str(o, "activity_kind")? {
        "chat" => ActivityKind::Chat,
        "coordination" => ActivityKind::Coordination,
        "automation" => ActivityKind::Automation,
        "delegation" => ActivityKind::Delegation,
        "maintenance" => ActivityKind::Maintenance,
        "evaluation" => ActivityKind::Evaluation,
        _ => return Err(ContextPlanError::invariant()),
    };
    Ok(RunContext {
        tenant_id: id(o, "tenant_id")?,
        conversation_id: id(o, "conversation_id")?,
        activity_id: id(o, "activity_id")?,
        activity_kind,
        run_id: id(o, "run_id")?,
        executor_participant_id: id(o, "executor_participant_id")?,
        requested_by_participant_id: id(o, "requested_by_participant_id")?,
        parent_run_id: optional(o, "parent_run_id", parse)?,
        delegation_id: optional(o, "delegation_id", parse)?,
        execution_scope: scope(field(o, "execution_scope")?)?,
        control_home: home(field(o, "control_home")?)?,
        execution_home: home(field(o, "execution_home")?)?,
        home_epoch: checked(HomeEpoch::new(required_u64(o, "home_epoch")?))?,
        runtime_snapshot_id: id(o, "runtime_snapshot_id")?,
    })
}
pub(super) fn attempt_context(v: &Value) -> Result<AttemptContext> {
    let o = object(v)?;
    Ok(AttemptContext {
        attempt_id: id(o, "attempt_id")?,
        run_id: id(o, "run_id")?,
        attempt_number: checked(AttemptNumber::new(checked(u32::try_from(required_u64(
            o,
            "attempt_number",
        )?))?))?,
        worker_id: checked(musteragent_run::WorkerId::new(required_str(
            o,
            "worker_id",
        )?))?,
        lease_id: id(o, "lease_id")?,
        fencing_token: checked(FencingToken::new(required_u64(o, "fencing_token")?))?,
        lease_expires_at: timestamp(o, "lease_expires_at_ms")?,
    })
}
fn home(v: &Value) -> Result<RuntimeHome> {
    let o = object(v)?;
    match required_str(o, "kind")? {
        "cloud" => Ok(RuntimeHome::Cloud),
        "device" => Ok(RuntimeHome::Device {
            device_id: id(o, "device_id")?,
        }),
        _ => Err(ContextPlanError::invariant()),
    }
}
fn scope(v: &Value) -> Result<ExecutionScope> {
    let o = object(v)?;
    Ok(match required_str(o, "kind")? {
        "tenant" => ExecutionScope::Tenant {
            tenant_id: id(o, "tenant_id")?,
        },
        "workspace" => ExecutionScope::Workspace {
            workspace_id: id(o, "workspace_id")?,
        },
        "device_private" => ExecutionScope::DevicePrivate {
            device_id: id(o, "device_id")?,
            workspace_id: optional(o, "workspace_id", parse)?,
        },
        "hosted_project" => {
            let h = object_field(o, "home")?;
            ExecutionScope::HostedProject {
                scope: musteragent_event_envelope::HostedProjectScope {
                    project_id: id(o, "project_id")?,
                    workspace_id: id(o, "workspace_id")?,
                    resource_tenant_id: id(o, "resource_tenant_id")?,
                    owner_principal_id: id(o, "owner_principal_id")?,
                    home: musteragent_event_envelope::HostedHomeBinding {
                        binding_id: id(h, "binding_id")?,
                        device_id: id(h, "device_id")?,
                        home_epoch: checked(HomeEpoch::new(required_u64(h, "home_epoch")?))?,
                    },
                },
            }
        }
        _ => return Err(ContextPlanError::invariant()),
    })
}
pub(super) fn route(v: &Value) -> Result<ModelRoute> {
    let o = object(v)?;
    let parameters = object_field(o, "parameters")?
        .iter()
        .map(|(k, v)| {
            let value = match v {
                Value::Bool(v) => ModelParameter::Bool(*v),
                Value::String(v) => ModelParameter::String(v.clone()),
                Value::Number(v) if v.is_i64() => {
                    ModelParameter::Int(v.as_i64().ok_or_else(ContextPlanError::invariant)?)
                }
                Value::Number(v) => ModelParameter::Double(
                    v.as_f64()
                        .filter(|v| v.is_finite())
                        .ok_or_else(ContextPlanError::invariant)?,
                ),
                _ => return Err(ContextPlanError::invariant()),
            };
            Ok((
                checked(musteragent_identity::DottedKey::new(k.clone()))?,
                value,
            ))
        })
        .collect::<Result<_>>()?;
    Ok(ModelRoute {
        route_ref: key(field(o, "route_ref")?)?,
        resolved_model_key: required_str(o, "resolved_model_key")?.to_owned(),
        model_provider_id: id(o, "model_provider_id")?,
        model_entry_id: id(o, "model_entry_id")?,
        api_format: checked(ApiFormat::parse_control_plane(required_str(
            o,
            "api_format",
        )?))?,
        adapter_version: key(field(o, "adapter_version")?)?,
        protocol_version: key(field(o, "protocol_version")?)?,
        context_window: positive_u64(o, "context_window")?,
        max_output_tokens: positive_u64(o, "max_output_tokens")?,
        capabilities: list(o, "capabilities", |v| {
            checked(musteragent_identity::DottedKey::new(string(v)?))
        })?
        .into_iter()
        .collect(),
        parameters,
        fallback_route_refs: list(o, "fallback_route_refs", key)?,
        pricing_version_ref: checked(PricingVersionRef::new(required_str(
            o,
            "pricing_version_ref",
        )?))?,
        provider_state_policy: match required_str(o, "provider_state_policy")? {
            "stateless" => ProviderStatePolicy::Stateless,
            "opaque_continuation" => ProviderStatePolicy::OpaqueContinuation,
            _ => return Err(ContextPlanError::invariant()),
        },
    })
}
