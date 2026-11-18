//! Original Compression store requests; identity bytes reuse canonical Context metadata.
use super::*;
use crate::context_codec::metadata;
use musteragent_context::{
    ContextCompactionDispatchState as State, ContextCompactionRecovery as Recovery,
    ContextCompactionRecoveryState as RecoveryState, PersistedContextCompactionSlot as Slot,
};
use musteragent_identity::UtcTimestamp;
use musteragent_loop::AgentCompactionCommand as Command;
use musteragent_runtime_model::ModelPersistenceDisposition;
pub(super) mod generation;
mod model;
mod request;

pub(super) fn encode(v: &Command) -> Result<wire::AgentCompactionCommand> {
    use wire::agent_compaction_command::Operation as W;
    Ok(wire::AgentCompactionCommand {
        operation: Some(match v {
            Command::BeginSupervised {
                request,
                invocation,
            } => W::BeginSupervised(Box::new(wire::AgentCompactionSeal {
                request: Some(request::encode(request)?),
                invocation: Some(model::encode(invocation)?),
            })),
            Command::CompleteSupervised(v) => {
                W::CompleteSupervised(Box::new(wire::AgentCompactionSupervisedCompletion {
                    canonical_invocation_metadata: checked(metadata::encode_invocation(
                        &v.context,
                    ))?,
                    completion: Some(model::completion(&v.completion)?),
                    native_terminal: v.native_terminal,
                }))
            }
            Command::LatestGeneration(v) => W::LatestGeneration(Box::new(request::encode(v)?)),
            Command::LatestGenerationWithSealedEstimate(v) => {
                W::LatestGenerationWithSealedEstimate(Box::new(request::encode(v)?))
            }
            Command::LatestRoleCandidate(v) => {
                W::LatestRoleCandidate(Box::new(request::encode(v)?))
            }
            Command::Load(v) => W::Load(Box::new(request::encode(v)?)),
            Command::Seal {
                request,
                invocation,
            } => W::Seal(Box::new(wire::AgentCompactionSeal {
                request: Some(request::encode(request)?),
                invocation: Some(model::encode(invocation)?),
            })),
            Command::Dispatch(v) => W::Dispatch(Box::new(request::encode(v)?)),
            Command::Recover(v) => W::Recover(wire::InvocationId {
                value: v.to_string(),
            }),
            Command::Complete(v) => W::Complete(Box::new(model::completion(v)?)),
            Command::ReadContent {
                invocation_id,
                content,
            } => W::ReadContent(wire::AgentCompactionReadContent {
                invocation_id: Some(wire::InvocationId {
                    value: invocation_id.to_string(),
                }),
                content: Some(crate::wire_common::content_ref(content)),
            }),
        }),
    })
}
pub(super) fn decode(v: wire::AgentCompactionCommand) -> Result<Command> {
    use wire::agent_compaction_command::Operation as W;
    Ok(match required(v.operation)? {
        W::BeginSupervised(v) => Command::BeginSupervised {
            request: Box::new(request::decode(required(v.request)?)?),
            invocation: Box::new(model::decode(required(v.invocation)?)?),
        },
        W::CompleteSupervised(v) => Command::CompleteSupervised(Box::new(
            musteragent_runtime_model::SupervisedModelCompletion {
                context: checked(metadata::decode_invocation(
                    &v.canonical_invocation_metadata,
                ))?,
                completion: model::decode_completion(required(v.completion)?)?,
                native_terminal: v.native_terminal,
            },
        )),
        W::LatestGeneration(v) => Command::LatestGeneration(Box::new(request::decode(*v)?)),
        W::LatestGenerationWithSealedEstimate(v) => {
            Command::LatestGenerationWithSealedEstimate(Box::new(request::decode(*v)?))
        }
        W::LatestRoleCandidate(v) => Command::LatestRoleCandidate(Box::new(request::decode(*v)?)),
        W::Load(v) => Command::Load(Box::new(request::decode(*v)?)),
        W::Seal(v) => Command::Seal {
            request: Box::new(request::decode(required(v.request)?)?),
            invocation: Box::new(model::decode(required(v.invocation)?)?),
        },
        W::Dispatch(v) => Command::Dispatch(Box::new(request::decode(*v)?)),
        W::Recover(v) => Command::Recover(id(Some(v.value))?),
        W::Complete(v) => Command::Complete(Box::new(model::decode_completion(*v)?)),
        W::ReadContent(v) => Command::ReadContent {
            invocation_id: id(v.invocation_id.map(|v| v.value))?,
            content: content(required(v.content)?)?,
        },
    })
}
pub(super) fn slot(v: &Slot) -> Result<wire::AgentCompactionPersistedSlot> {
    Ok(wire::AgentCompactionPersistedSlot {
        canonical_invocation_metadata: checked(metadata::encode_invocation(&v.invocation))?,
        request_ref: Some(crate::wire_common::content_ref(&v.request_ref)),
        request_hash: Some(hash_wire(v.request_hash)),
        model_route_ref: v.model_route_ref.as_str().to_owned(),
        started_at_ms: v.started_at.unix_millis(),
        dispatch_state: match v.dispatch_state {
            State::SealedNotDispatched => 1,
            State::Dispatched => 2,
            State::Terminal => 3,
        },
    })
}
pub(super) fn decode_slot(v: wire::AgentCompactionPersistedSlot) -> Result<Slot> {
    let value = Slot {
        invocation: checked(metadata::decode_invocation(
            &v.canonical_invocation_metadata,
        ))?,
        request_ref: content(required(v.request_ref)?)?,
        request_hash: hash(required(v.request_hash)?)?,
        model_route_ref: route_ref(Some(v.model_route_ref))?,
        started_at: UtcTimestamp::from_unix_millis(v.started_at_ms),
        dispatch_state: match v.dispatch_state {
            1 => State::SealedNotDispatched,
            2 => State::Dispatched,
            3 => State::Terminal,
            _ => return Err(AgentContextCodecError::Invalid),
        },
    };
    if value.request_ref.sha256 != value.request_hash {
        return Err(AgentContextCodecError::Invalid);
    }
    Ok(value)
}
pub(super) fn recovery(v: &Recovery) -> Result<wire::AgentCompactionRecovery> {
    let (state, response, error_code) = match &v.state {
        RecoveryState::SealedNotDispatched => (1, None, None),
        RecoveryState::Indeterminate => (2, None, None),
        RecoveryState::Succeeded(v) => (3, Some(crate::wire_common::content_ref(v)), None),
        RecoveryState::Failed(v) => (4, None, Some(v.as_str().to_owned())),
        RecoveryState::Cancelled => (5, None, None),
    };
    Ok(wire::AgentCompactionRecovery {
        early_provider_cause: v
            .early_provider_cause
            .map(|cause| cause.as_str().to_owned()),
        failure_retryable: v.failure_retryable,
        retry_not_before_ms: v.retry_not_before.map(|v| v.unix_millis()),
        request_hash: Some(hash_wire(v.request_hash)),
        model_route_ref: v.model_route_ref.as_str().to_owned(),
        state,
        response,
        error_code,
    })
}
pub(super) fn decode_recovery(v: wire::AgentCompactionRecovery) -> Result<Recovery> {
    let state = match v.state {
        1 if v.response.is_none() && v.error_code.is_none() => RecoveryState::SealedNotDispatched,
        2 if v.response.is_none() && v.error_code.is_none() => RecoveryState::Indeterminate,
        3 if v.error_code.is_none() => RecoveryState::Succeeded(content(required(v.response)?)?),
        4 if v.response.is_none() => RecoveryState::Failed(required(
            musteragent_runtime_contract::ErrorCode::from_safe_code(&required(v.error_code)?),
        )?),
        5 if v.response.is_none() && v.error_code.is_none() => RecoveryState::Cancelled,
        _ => return Err(AgentContextCodecError::Invalid),
    };
    Ok(Recovery {
        early_provider_cause: v
            .early_provider_cause
            .as_deref()
            .map(|cause| {
                musteragent_runtime_model::ProviderErrorCode::from_alternate_provider_cause(cause)
                    .ok_or(AgentContextCodecError::Invalid)
            })
            .transpose()?,
        failure_retryable: v.failure_retryable,
        retry_not_before: v
            .retry_not_before_ms
            .map(musteragent_identity::UtcTimestamp::from_unix_millis),
        request_hash: hash(required(v.request_hash)?)?,
        model_route_ref: route_ref(Some(v.model_route_ref))?,
        state,
    })
}
pub(super) fn disposition(v: ModelPersistenceDisposition) -> u32 {
    match v {
        ModelPersistenceDisposition::Applied => 1,
        ModelPersistenceDisposition::Duplicate => 2,
    }
}
pub(super) fn decode_disposition(v: u32) -> Result<ModelPersistenceDisposition> {
    match v {
        1 => Ok(ModelPersistenceDisposition::Applied),
        2 => Ok(ModelPersistenceDisposition::Duplicate),
        _ => Err(AgentContextCodecError::Invalid),
    }
}
