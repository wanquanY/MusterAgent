//! Canonical interaction Ledger facts. Wire validation is unchanged.
use super::*;

pub(super) fn decode(value: wire::domain_event::Event) -> Result<DomainEvent, SubscriptionError> {
    use wire::domain_event::Event;
    match value {
        Event::InteractionRequested(value) => {
            Ok(DomainEvent::Interaction(InteractionEvent::Requested {
                interaction_id: required_id(value.interaction_id.map(|value| value.value))?,
                invocation_id: required_id(value.invocation_id.map(|value| value.value))?,
                kind: interaction_kind(value.kind)?,
                prompt_ref: content_ref(value.prompt_ref.ok_or_else(corrupt)?)?,
                response_schema: schema_ref(value.response_schema)?,
                expires_at: value.expires_at.map(timestamp).transpose()?,
            }))
        }
        Event::InteractionResponded(value) => {
            Ok(DomainEvent::Interaction(InteractionEvent::Responded {
                interaction_id: required_id(value.interaction_id.map(|value| value.value))?,
                responded_by: required_id(value.responded_by.map(|value| value.value))?,
                response: interaction_response(value.response)?,
                responded_at: timestamp(value.responded_at.ok_or_else(corrupt)?)?,
            }))
        }
        Event::InteractionExpired(value) => {
            Ok(DomainEvent::Interaction(InteractionEvent::Expired {
                interaction_id: required_id(value.interaction_id.map(|value| value.value))?,
                expired_at: timestamp(value.expired_at.ok_or_else(corrupt)?)?,
            }))
        }
        Event::InteractionCancelled(value) => {
            Ok(DomainEvent::Interaction(InteractionEvent::Cancelled {
                interaction_id: required_id(value.interaction_id.map(|value| value.value))?,
                reason: cancel_reason(value.reason)?,
            }))
        }
        _ => Err(corrupt()),
    }
}
