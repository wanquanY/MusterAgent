//! Canonical discovery Ledger facts. Wire validation is unchanged.
use super::*;

pub(super) fn decode(value: wire::domain_event::Event) -> Result<DomainEvent, SubscriptionError> {
    use wire::domain_event::Event;
    match value {
        Event::ToolExposureAdded(value) => Ok(DomainEvent::Discovery(
            DiscoveryEvent::ToolExposureAdded(ToolExposureAdded {
                exposures: value
                    .exposures
                    .into_iter()
                    .map(|exposure| {
                        Ok(ToolExposureItem {
                            tool_ref: ToolKey::from_versioned_key(
                                VersionedKey::new(exposure.tool_ref.ok_or_else(corrupt)?.key)
                                    .map_err(|_| corrupt())?,
                            ),
                            schema_hash: sha256(exposure.schema_hash)?,
                            ordinal: NonZeroU32::new(exposure.ordinal).ok_or_else(corrupt)?,
                        })
                    })
                    .collect::<Result<_, SubscriptionError>>()?,
                reason_ref: content_ref(value.reason_ref.ok_or_else(corrupt)?)?,
                source_invocation_id: required_id(
                    value.source_invocation_id.map(|value| value.value),
                )?,
                exposed_at: timestamp(value.exposed_at.ok_or_else(corrupt)?)?,
            }),
        )),
        Event::SkillDisclosed(value) => Ok(DomainEvent::Discovery(DiscoveryEvent::SkillDisclosed(
            SkillDisclosed {
                skill_ref: VersionedKey::new(value.skill_ref.ok_or_else(corrupt)?.key)
                    .map_err(|_| corrupt())?,
                instructions_ref: content_ref(value.instructions_ref.ok_or_else(corrupt)?)?,
                instructions_hash: sha256(value.instructions_hash)?,
                disclosure_ordinal: NonZeroU32::new(value.disclosure_ordinal)
                    .ok_or_else(corrupt)?,
                source_invocation_id: required_id(
                    value.source_invocation_id.map(|value| value.value),
                )?,
                disclosed_at: timestamp(value.disclosed_at.ok_or_else(corrupt)?)?,
            },
        ))),
        _ => Err(corrupt()),
    }
}
