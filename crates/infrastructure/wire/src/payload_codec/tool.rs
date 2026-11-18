//! Canonical tool Ledger facts. Wire validation is unchanged.
use super::*;

pub(super) fn decode(value: wire::domain_event::Event) -> Result<DomainEvent, SubscriptionError> {
    use wire::domain_event::Event;
    match value {
        Event::ToolSandboxRouted(value) => {
            use musteragent_sandbox::{
                SandboxCompatibility, SandboxExecutor, SandboxRouteDecision, SandboxRouteReason,
                SandboxRouteRecord,
            };
            let route = value.route.ok_or_else(corrupt)?;
            let route = SandboxRouteRecord {
                decision: SandboxRouteDecision {
                    executor: SandboxExecutor::parse(&route.executor).map_err(|_| corrupt())?,
                    reason: SandboxRouteReason::parse(&route.reason).map_err(|_| corrupt())?,
                    compatibility: SandboxCompatibility::parse(&route.compatibility)
                        .map_err(|_| corrupt())?,
                },
                decided_at: timestamp(route.decided_at.ok_or_else(corrupt)?)?,
            };
            route.validate().map_err(|_| corrupt())?;
            Ok(DomainEvent::Tool(ToolEvent::SandboxRouted {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                route,
            }))
        }
        Event::ToolInvocationRequested(value) => {
            Ok(DomainEvent::Tool(ToolEvent::InvocationRequested {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                tool_call_id: ToolCallId::new(value.tool_call_id.ok_or_else(corrupt)?.value)
                    .map_err(|_| corrupt())?,
                tool_key: ToolKey::from_versioned_key(
                    VersionedKey::new(value.tool_ref.ok_or_else(corrupt)?.key)
                        .map_err(|_| corrupt())?,
                ),
                effect: effect(value.effect)?,
                request_ref: content_ref(value.request_ref.ok_or_else(corrupt)?)?,
                timing: crate::optional_invocation_execution_timing(
                    value.deadline,
                    value.activity_timing,
                )
                .map_err(|_| corrupt())?
                .ok_or_else(corrupt)?,
            }))
        }
        Event::ToolInvocationWaitingInteraction(value) => {
            Ok(DomainEvent::Tool(ToolEvent::InvocationWaitingInteraction {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                interaction_id: required_id(value.interaction_id.map(|value| value.value))?,
            }))
        }
        Event::ToolInvocationStarted(value) => {
            Ok(DomainEvent::Tool(ToolEvent::InvocationStarted {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                execution_attempt_id: required_id(
                    value.execution_attempt_id.map(|value| value.value),
                )?,
                capability_provider_id: required_id(
                    value.capability_provider_id.map(|value| value.value),
                )?,
                execution_timing: crate::optional_invocation_execution_timing(
                    value.execution_deadline,
                    value.activity_timing,
                )
                .map_err(|_| corrupt())?,
            }))
        }
        Event::ToolInvocationSucceeded(value) => {
            Ok(DomainEvent::Tool(ToolEvent::InvocationSucceeded {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                result_ref: content_ref(value.result_ref.ok_or_else(corrupt)?)?,
                result_hash: sha256(value.result_hash)?,
                provider_request_ref: value
                    .provider_request_ref
                    .map(OpaqueExternalRef::new)
                    .transpose()
                    .map_err(|_| corrupt())?,
                authorized_content: value
                    .authorized_content
                    .into_iter()
                    .map(content_ref)
                    .collect::<Result<Vec<_>, _>>()?,
            }))
        }
        Event::ToolInvocationFailed(value) => Ok(DomainEvent::Tool(ToolEvent::InvocationFailed {
            invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
            error: error_info(value.error)?,
            retry_disposition: retry_disposition(value.retry_disposition)?,
            diagnostic_result_ref: value.diagnostic_result_ref.map(content_ref).transpose()?,
            diagnostic_result_hash: value
                .diagnostic_result_hash
                .map(|value| sha256(Some(value)))
                .transpose()?,
        })),
        Event::ToolInvocationIndeterminate(value) => {
            Ok(DomainEvent::Tool(ToolEvent::InvocationIndeterminate {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                reason: SafeDisplayText::new(value.reason).map_err(|_| corrupt())?,
                reconciliation_ref: OpaqueExternalRef::new(value.reconciliation_ref)
                    .map_err(|_| corrupt())?,
                observed_error: value
                    .observed_error
                    .map(|value| error_info(Some(value)))
                    .transpose()?,
                diagnostic_result_ref: value.diagnostic_result_ref.map(content_ref).transpose()?,
                diagnostic_result_hash: value
                    .diagnostic_result_hash
                    .map(|value| sha256(Some(value)))
                    .transpose()?,
            }))
        }
        Event::ToolInvocationCancelled(value) => {
            Ok(DomainEvent::Tool(ToolEvent::InvocationCancelled {
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                reason: cancel_reason(value.reason)?,
            }))
        }
        _ => Err(corrupt()),
    }
}
