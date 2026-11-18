use super::*;
use musteragent_context::{ContextSourceOwner, ContextSourceSelector};
use musteragent_runtime_snapshot::ContextSourceSelectionPolicy;

pub(super) fn selector(
    run: &RunContext,
    boundary: ConversationVisibilityBoundary,
    policy: ContextSourceSelectionPolicy,
) -> ContextSourceSelector {
    ContextSourceSelector {
        policy,
        boundary,
        target: ContextSourceOwner {
            run_id: run.run_id,
            activity_id: run.activity_id,
            activity_kind: run.activity_kind,
            delegation_id: run.delegation_id,
            admitted_sequence: boundary.admitted_sequence,
        },
    }
}

pub(super) fn includes(
    selector: ContextSourceSelector,
    row: &sqlx::postgres::PgRow,
    sequence: ConversationSeq,
    run_id: Option<RunId>,
    admitted_sequence: Option<ConversationSeq>,
    record_activity_id: Option<ActivityId>,
) -> Result<bool, PostgresContextMaterialError> {
    if selector.policy == ContextSourceSelectionPolicy::LegacyAdmissionV1 {
        return Ok(selector.includes(sequence, run_id, admitted_sequence, None));
    }
    let owner = match (run_id, admitted_sequence) {
        (Some(run_id), Some(admitted_sequence)) => ContextSourceOwner {
            run_id,
            admitted_sequence,
            activity_id: id(row.try_get("source_activity_id")?)?,
            activity_kind: activity_kind(row.try_get("source_activity_kind")?)?,
            delegation_id: row
                .try_get::<Option<uuid::Uuid>, _>("source_delegation_id")?
                .map(id)
                .transpose()?,
        },
        _ => {
            return Err(PostgresContextMaterialError::CorruptStateAt(
                "source_run_owner",
            ));
        }
    };
    if record_activity_id.is_some_and(|activity_id| activity_id != owner.activity_id) {
        return Err(PostgresContextMaterialError::CorruptStateAt(
            "source_run_owner",
        ));
    }
    Ok(selector.includes(sequence, run_id, admitted_sequence, Some(owner)))
}

pub(super) fn model_bearing_event(event: &DomainEvent) -> bool {
    matches!(
        event,
        DomainEvent::Conversation(
            ConversationEvent::MessageAccepted { .. }
                | ConversationEvent::MessageImported { .. }
                | ConversationEvent::MessageCommitted { .. }
                | ConversationEvent::MessagePartiallyCommitted { .. }
        ) | DomainEvent::Model(_)
            | DomainEvent::Tool(_)
            | DomainEvent::Interaction(_)
            | DomainEvent::Discovery(_)
    )
}

pub(super) fn register_excluded_record(
    facts: &mut LedgerFacts,
    record_id: LedgerRecordId,
    sequence: ConversationSeq,
) -> Result<(), PostgresContextMaterialError> {
    if facts
        .records
        .insert(
            record_id,
            RecordSource {
                sequence,
                branch_id: facts.active_branch_id,
            },
        )
        .is_some()
    {
        return Err(PostgresContextMaterialError::CorruptState);
    }
    Ok(())
}
