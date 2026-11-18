//! Canonical source records. Decoding is not source authority or Goal admission.
use crate::{
    dialogue_codec::{DialogueCodecError, id, required},
    wire,
};
use musteragent_content_ref::Sha256;
use musteragent_goal::{event_subscription::*, source_binding::GoalSourceSubject};
use musteragent_identity::{UtcTimestamp, VersionedKey};
use std::collections::BTreeSet;

type Error = DialogueCodecError;
const BAD: Error = Error::Corrupt;

pub fn desired(
    source: &OwnerEventSubscription,
    goal_revision: u64,
    capture_requested: bool,
    cursor: musteragent_runtime_session::AccountCursor,
) -> Result<wire::OwnerEventSourceDesired, Error> {
    source_guard::revision(goal_revision)?;
    source_guard::revision(cursor.epoch)?;
    if cursor.seq > i64::MAX as u64 {
        return Err(BAD);
    }
    Ok(wire::OwnerEventSourceDesired {
        subscription: Some(subscription(source)?),
        goal_revision,
        capture_requested,
        account_sequence: cursor.seq,
        account_epoch: cursor.epoch,
        configuration_sha256: source.configuration.fingerprint().into_bytes().to_vec(),
        account_space_id: Some(wire::OwnerSpaceId {
            value: cursor.space_id.to_string(),
        }),
    })
}

pub fn input_receipt(
    value: &musteragent_runtime_session::owner_event_sources::OwnerSourceInputReceipt,
) -> Result<wire::OwnerSourceInputReceipt, Error> {
    let encoded = wire::OwnerSourceInputReceipt {
        source_observation: value.source_observation.as_ref().map(observation),
        goal_id: Some(wire::GoalId {
            value: value.goal_id.to_string(),
        }),
        event_id: Some(wire::SourceEventId {
            value: value.event_id.to_string(),
        }),
        evidence_sha256: value.evidence_digest.into_bytes().to_vec(),
        source_object: value.source_object.as_ref().map(source_object),
        input_sequence: value.input_sequence.get(),
        matches: value
            .matches
            .iter()
            .map(|entry| wire::SourceInputMatch {
                subscription_id: Some(wire::OwnerEventSubscriptionId {
                    value: entry.subscription_id.to_string(),
                }),
                guard: Some(guard(entry.guard)),
            })
            .collect(),
    };
    decode_input_receipt(encoded.clone())?;
    Ok(encoded)
}

pub fn decode_input_receipt(
    value: wire::OwnerSourceInputReceipt,
) -> Result<musteragent_runtime_session::owner_event_sources::OwnerSourceInputReceipt, Error> {
    use musteragent_runtime_session::owner_event_sources::*;
    if value.matches.is_empty() || value.matches.len() > MAX_GOAL_EVENT_SUBSCRIPTIONS {
        return Err(BAD);
    }
    let mut seen = BTreeSet::new();
    let matches = value
        .matches
        .into_iter()
        .map(|entry| {
            let subscription_id = id(entry.subscription_id.map(|id| id.value))?;
            if !seen.insert(subscription_id) {
                return Err(BAD);
            }
            Ok(SourceInputMatch {
                subscription_id,
                guard: decode_guard(entry.guard)?,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let receipt = OwnerSourceInputReceipt {
        source_observation: value
            .source_observation
            .map(decode_observation)
            .transpose()?,
        goal_id: id(value.goal_id.map(|id| id.value))?,
        event_id: id(value.event_id.map(|id| id.value))?,
        evidence_digest: Sha256::from_bytes(value.evidence_sha256).map_err(|_| BAD)?,
        source_object: value.source_object.map(decode_source_object).transpose()?,
        input_sequence: source_guard::revision(value.input_sequence)?,
        matches,
    };
    if receipt
        .source_observation
        .as_ref()
        .is_some_and(|observation| {
            observation.event_id != receipt.event_id
                || observation.evidence_digest != receipt.evidence_digest
                || observation.source_object != receipt.source_object
        })
    {
        return Err(BAD);
    }
    Ok(receipt)
}

pub fn guard(value: EventSubscriptionGuard) -> wire::OwnerEventSubscriptionGuard {
    wire::OwnerEventSubscriptionGuard {
        revision: value.revision.get(),
        activation_epoch: value.activation_epoch.get(),
    }
}

pub fn decode_intent(value: i32) -> Result<EventSubscriptionIntent, Error> {
    match value {
        1 => Ok(EventSubscriptionIntent::Watching),
        2 => Ok(EventSubscriptionIntent::Paused),
        3 => Ok(EventSubscriptionIntent::Ended),
        _ => Err(BAD),
    }
}
pub fn decode_guard(
    value: Option<wire::OwnerEventSubscriptionGuard>,
) -> Result<EventSubscriptionGuard, Error> {
    let value = required(value)?;
    if value.activation_epoch > value.revision {
        return Err(BAD);
    }
    Ok(EventSubscriptionGuard {
        revision: source_guard::revision(value.revision)?,
        activation_epoch: source_guard::revision(value.activation_epoch)?,
    })
}
pub fn configuration(
    value: &EventSubscriptionConfiguration,
) -> Result<wire::OwnerEventSubscriptionConfiguration, Error> {
    value.validate().map_err(|_| BAD)?;
    Ok(wire::OwnerEventSubscriptionConfiguration {
        application_binding_id: Some(wire::OwnerApplicationBindingId {
            value: value.application_binding_id.to_string(),
        }),
        application_guard: Some(source_guard::guard(value.application_guard)),
        capability_ref: value.capability_ref.as_str().to_owned(),
        capability_sha256: value.capability_digest.into_bytes().to_vec(),
        selection_json: value.selection.json_bytes(),
        event_kinds: value
            .event_kinds
            .iter()
            .map(|kind| kind.as_str().to_owned())
            .collect(),
    })
}
pub fn decode_configuration(
    value: Option<wire::OwnerEventSubscriptionConfiguration>,
) -> Result<EventSubscriptionConfiguration, Error> {
    let value = required(value)?;
    if value.event_kinds.len() > MAX_SUBSCRIBED_EVENT_KINDS {
        return Err(BAD);
    }
    let count = value.event_kinds.len();
    let event_kinds = value
        .event_kinds
        .into_iter()
        .map(|key| VersionedKey::new(key).map_err(|_| BAD))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if count != event_kinds.len() {
        return Err(BAD);
    }
    let value = EventSubscriptionConfiguration {
        application_binding_id: id(value.application_binding_id.map(|id| id.value))?,
        application_guard: source_guard::decode_guard(value.application_guard)?,
        capability_ref: VersionedKey::new(value.capability_ref).map_err(|_| BAD)?,
        capability_digest: Sha256::from_bytes(value.capability_sha256).map_err(|_| BAD)?,
        selection: EventSourceSelection::from_json(&value.selection_json).map_err(|_| BAD)?,
        event_kinds,
    };
    value.validate().map_err(|_| BAD)?;
    Ok(value)
}
pub fn boundary(value: &SourceActivationBoundary) -> wire::SourceActivationBoundary {
    wire::SourceActivationBoundary {
        boundary_id: Some(wire::SourceBoundaryId {
            value: value.boundary_id.to_string(),
        }),
        subscription_id: Some(wire::OwnerEventSubscriptionId {
            value: value.subscription_id.to_string(),
        }),
        activation_epoch: value.activation_epoch.get(),
        configuration_sha256: value.configuration_digest.into_bytes().to_vec(),
        capture_id: Some(wire::SourceCaptureId {
            value: value.capture_id.to_string(),
        }),
        capture_generation: value.capture_generation.get(),
        after_source_sequence: value.after_source_sequence,
        source_status_revision: value.source_status_revision.get(),
        evidence_sha256: value.evidence_digest.into_bytes().to_vec(),
    }
}
pub fn decode_boundary(
    value: wire::SourceActivationBoundary,
) -> Result<SourceActivationBoundary, Error> {
    if value.after_source_sequence > i64::MAX as u64 {
        return Err(BAD);
    }
    Ok(SourceActivationBoundary {
        boundary_id: id(value.boundary_id.map(|id| id.value))?,
        subscription_id: id(value.subscription_id.map(|id| id.value))?,
        activation_epoch: source_guard::revision(value.activation_epoch)?,
        configuration_digest: Sha256::from_bytes(value.configuration_sha256).map_err(|_| BAD)?,
        capture_id: id(value.capture_id.map(|id| id.value))?,
        capture_generation: source_guard::revision(value.capture_generation)?,
        after_source_sequence: value.after_source_sequence,
        source_status_revision: source_guard::revision(value.source_status_revision)?,
        evidence_digest: Sha256::from_bytes(value.evidence_sha256).map_err(|_| BAD)?,
    })
}
pub fn readiness(value: &SourceReadinessObservation) -> wire::SourceReadinessObservation {
    wire::SourceReadinessObservation {
        capture_id: Some(wire::SourceCaptureId {
            value: value.capture_id.to_string(),
        }),
        capture_generation: value.capture_generation.get(),
        revision: value.revision.get(),
        readiness: match value.readiness {
            SourceReadiness::Ready => 1,
            SourceReadiness::Retrying => 2,
            SourceReadiness::AuthorizationRequired => 3,
            SourceReadiness::CoverageGap => 4,
            SourceReadiness::Retired => 5,
        },
        evidence_sha256: value.evidence_digest.into_bytes().to_vec(),
    }
}
pub fn decode_readiness(
    value: wire::SourceReadinessObservation,
) -> Result<SourceReadinessObservation, Error> {
    Ok(SourceReadinessObservation {
        capture_id: id(value.capture_id.map(|id| id.value))?,
        capture_generation: source_guard::revision(value.capture_generation)?,
        revision: source_guard::revision(value.revision)?,
        readiness: match value.readiness {
            1 => SourceReadiness::Ready,
            2 => SourceReadiness::Retrying,
            3 => SourceReadiness::AuthorizationRequired,
            4 => SourceReadiness::CoverageGap,
            5 => SourceReadiness::Retired,
            _ => return Err(BAD),
        },
        evidence_digest: Sha256::from_bytes(value.evidence_sha256).map_err(|_| BAD)?,
    })
}
pub fn source_object(value: &SourceObjectIdentity) -> wire::SourceObjectIdentity {
    wire::SourceObjectIdentity {
        provider: value.provider.clone(),
        connection: value.connection.clone(),
        resource: value.resource.clone(),
        version: value.version.clone(),
        observed_at_unix_ms: value.observed_at.unix_millis(),
    }
}
pub fn decode_source_object(
    value: wire::SourceObjectIdentity,
) -> Result<SourceObjectIdentity, Error> {
    let result = SourceObjectIdentity {
        provider: value.provider,
        connection: value.connection,
        resource: value.resource,
        version: value.version,
        observed_at: UtcTimestamp::from_unix_millis(value.observed_at_unix_ms),
    };
    result.validate().map_err(|_| BAD)?;
    Ok(result)
}
pub fn observation(value: &SourceObservationIdentity) -> wire::SourceObservationIdentity {
    wire::SourceObservationIdentity {
        capture_id: Some(wire::SourceCaptureId {
            value: value.capture_id.to_string(),
        }),
        capture_generation: value.capture_generation.get(),
        event_id: Some(wire::SourceEventId {
            value: value.event_id.to_string(),
        }),
        source_object: value.source_object.as_ref().map(source_object),
        source_sequence: value.source_sequence.get(),
        event_kind: value.event_kind.as_str().to_owned(),
        evidence_sha256: value.evidence_digest.into_bytes().to_vec(),
    }
}
pub fn decode_observation(
    value: wire::SourceObservationIdentity,
) -> Result<SourceObservationIdentity, Error> {
    Ok(SourceObservationIdentity {
        capture_id: id(value.capture_id.map(|id| id.value))?,
        capture_generation: source_guard::revision(value.capture_generation)?,
        event_id: id(value.event_id.map(|id| id.value))?,
        source_object: value.source_object.map(decode_source_object).transpose()?,
        source_sequence: source_guard::revision(value.source_sequence)?,
        event_kind: VersionedKey::new(value.event_kind).map_err(|_| BAD)?,
        evidence_digest: Sha256::from_bytes(value.evidence_sha256).map_err(|_| BAD)?,
    })
}
pub fn subscription(value: &OwnerEventSubscription) -> Result<wire::OwnerEventSubscription, Error> {
    value.validate().map_err(|_| BAD)?;
    Ok(wire::OwnerEventSubscription {
        subscription_id: Some(wire::OwnerEventSubscriptionId {
            value: value.subscription_id.to_string(),
        }),
        goal_id: Some(wire::GoalId {
            value: value.goal_id.to_string(),
        }),
        tenant_id: Some(wire::TenantId {
            value: value.subject.tenant_id.to_string(),
        }),
        owner_id: Some(wire::PrincipalId {
            value: value.subject.owner_id.to_string(),
        }),
        agent_id: Some(wire::AgentId {
            value: value.subject.agent_id.to_string(),
        }),
        configuration: Some(configuration(&value.configuration)?),
        required: value.required,
        guard: Some(guard(value.guard)),
        desired: match value.desired {
            EventSubscriptionIntent::Watching => 1,
            EventSubscriptionIntent::Paused => 2,
            EventSubscriptionIntent::Ended => 3,
        },
        boundary: value.boundary.as_ref().map(boundary),
        source_status: value.source_status.as_ref().map(readiness),
        source_acknowledged_at_ms: value.source_acknowledged_at.map(UtcTimestamp::unix_millis),
        created_at_ms: value.created_at.unix_millis(),
        updated_at_ms: value.updated_at.unix_millis(),
    })
}
pub fn decode_subscription(
    value: Option<wire::OwnerEventSubscription>,
) -> Result<OwnerEventSubscription, Error> {
    let value = required(value)?;
    let value = OwnerEventSubscription {
        subscription_id: id(value.subscription_id.map(|id| id.value))?,
        goal_id: id(value.goal_id.map(|id| id.value))?,
        subject: GoalSourceSubject {
            tenant_id: id(value.tenant_id.map(|id| id.value))?,
            owner_id: id(value.owner_id.map(|id| id.value))?,
            agent_id: id(value.agent_id.map(|id| id.value))?,
        },
        configuration: decode_configuration(value.configuration)?,
        required: value.required,
        guard: decode_guard(value.guard)?,
        desired: decode_intent(value.desired)?,
        boundary: value.boundary.map(decode_boundary).transpose()?,
        source_status: value.source_status.map(decode_readiness).transpose()?,
        source_acknowledged_at: value
            .source_acknowledged_at_ms
            .map(UtcTimestamp::from_unix_millis),
        created_at: UtcTimestamp::from_unix_millis(value.created_at_ms),
        updated_at: UtcTimestamp::from_unix_millis(value.updated_at_ms),
    };
    value.validate().map_err(|_| BAD)?;
    Ok(value)
}

#[cfg(test)]
mod tests;

mod source_guard {
    use super::*;
    use musteragent_goal::source_binding::GoalSourceBindingGuard;
    use std::num::NonZeroU64;
    pub fn revision(value: u64) -> Result<NonZeroU64, Error> {
        if value > i64::MAX as u64 {
            return Err(BAD);
        }
        NonZeroU64::new(value).ok_or(BAD)
    }
    pub fn guard(value: GoalSourceBindingGuard) -> wire::OwnerApplicationGuard {
        wire::OwnerApplicationGuard {
            revision: value.revision.get(),
            authority_epoch: value.authority_epoch.get(),
        }
    }
    pub fn decode_guard(
        value: Option<wire::OwnerApplicationGuard>,
    ) -> Result<GoalSourceBindingGuard, Error> {
        let value = required(value)?;
        if value.authority_epoch > value.revision {
            return Err(BAD);
        }
        Ok(GoalSourceBindingGuard {
            revision: revision(value.revision)?,
            authority_epoch: revision(value.authority_epoch)?,
        })
    }
}
