//! Read current source authority inside the existing account/Goal transaction.
//! Returned evidence is historical delivery metadata, never an admission grant.
use super::*;

pub(crate) async fn proof(
    tx: &mut PgConnection,
    goal: &StandingGoal,
) -> Result<Option<serde_json::Value>, Error> {
    if goal.status != StandingGoalStatus::Active || goal.mode != StandingGoalMode::Continuous {
        return Ok(None);
    }
    let mut applications = record::ApplicationReadSet::new();
    let mut evidence = Vec::new();
    for source in record::list(tx, goal).await? {
        if source.desired == EventSubscriptionIntent::Ended {
            continue;
        }
        let observed = source.desired == EventSubscriptionIntent::Watching
            && source
                .boundary
                .as_ref()
                .is_some_and(|boundary| boundary.activation_epoch == source.guard.activation_epoch)
            && source
                .source_status
                .as_ref()
                .is_some_and(|status| status.readiness == SourceReadiness::Ready);
        if !observed {
            if source.required {
                return Ok(None);
            }
            continue;
        }
        match record::current_application(tx, &mut applications, &source).await {
            Ok(binding) if source.authorize(binding).is_ok() => (),
            Ok(_) | Err(Error::Forbidden | Error::NotFound) => {
                if source.required {
                    return Ok(None);
                }
                continue;
            }
            Err(error) => return Err(error),
        }
        // record::list decodes and validates the exact boundary/configuration
        // digest, status capture generation and current activation epoch.
        let boundary = source.boundary.as_ref().ok_or(Error::Corrupt)?;
        let status = source.source_status.as_ref().ok_or(Error::Corrupt)?;
        evidence.push(serde_json::json!({
            "subscription_id": source.subscription_id.to_string(),
            "revision": source.guard.revision.get().to_string(),
            "activation_epoch": source.guard.activation_epoch.get().to_string(),
            "required": source.required,
            "application_binding_id": source.configuration.application_binding_id.to_string(),
            "application_revision": source.configuration.application_guard.revision.get().to_string(),
            "application_authority_epoch": source.configuration.application_guard.authority_epoch.get().to_string(),
            "configuration_digest": boundary.configuration_digest.to_string(),
            "boundary_id": boundary.boundary_id.to_string(),
            "capture_id": boundary.capture_id.to_string(),
            "capture_generation": boundary.capture_generation.get().to_string(),
            "after_source_sequence": boundary.after_source_sequence.to_string(),
            "boundary_status_revision": boundary.source_status_revision.get().to_string(),
            "boundary_evidence_digest": boundary.evidence_digest.to_string(),
            "readiness_revision": status.revision.get().to_string(),
            "readiness_evidence_digest": status.evidence_digest.to_string()
        }));
    }
    // Empty required sets alone prove nothing. At least one current source
    // must have really acknowledged its Watching boundary and READY state.
    Ok((!evidence.is_empty()).then_some(serde_json::Value::Array(evidence)))
}
