//! Durable orchestration stop through the original cancellation contract.
//! Event construction is restored from the application cancellation owner.
use crate::*;
use musteragent_content_ref::Sha256;
use musteragent_event_envelope::*;
use musteragent_identity::*;
use musteragent_ledger::*;
use musteragent_runtime_automation::ScheduleServiceError as Error;
use musteragent_runtime_contract::CancelReason;
use musteragent_runtime_session::RuntimeApplicationStore;
use std::sync::Arc;

pub(crate) async fn cancel(
    database: &PostgresDatabaseRuntime,
    encoder: &dyn LedgerPayloadEncoder,
    actor: Actor,
    run_id: RunId,
    command: uuid::Uuid,
    reason: CancelReason,
    now: UtcTimestamp,
) -> Result<(), Error> {
    let store = PostgresRuntimeApplicationStore::new(
        database.clone(),
        Arc::new(musteragent_persistence_codec::ProtobufSnapshotCodec),
    );
    let correlation = CorrelationId::from_uuid(command).map_err(|_| Error::Corrupt)?;
    let terminal: bool = sqlx::query_scalar(
        "SELECT status IN (5,6,7,8) FROM runtime.runs WHERE run_id=$1 AND tenant_id=$2",
    )
    .bind(run_id.into_uuid())
    .bind(actor.tenant_id.into_uuid())
    .fetch_optional(database.pool())
    .await
    .map_err(|_| Error::Unavailable)?
    .ok_or(Error::NotFound)?;
    if terminal {
        return Ok(());
    }
    let authority = store
        .authorize_cancel_run(actor, run_id, correlation)
        .await
        .map_err(|_| Error::Unavailable)?;
    let mut events = Vec::new();
    if let Some(interaction_id) = authority.pending_interaction_id {
        events.push(DomainEvent::Interaction(
            musteragent_approval::InteractionEvent::Cancelled {
                interaction_id,
                reason,
            },
        ));
    }
    if let Some(invocation_id) = authority.pending_tool_invocation_id {
        events.push(DomainEvent::Tool(
            musteragent_tool::ToolEvent::InvocationCancelled {
                invocation_id,
                reason,
            },
        ));
    }
    events.push(DomainEvent::Run(
        musteragent_run::RunEvent::CancelRequested {
            reason,
            requested_by: actor.principal_id,
        },
    ));
    if authority.mode == CancelMode::CancelWithoutAttempt {
        events.push(DomainEvent::Run(musteragent_run::RunEvent::Cancelled {
            reason,
            last_attempt_id: authority.last_attempt_id,
        }));
        events.push(DomainEvent::Activity(
            musteragent_activity::ActivityEvent::Cancelled { reason },
        ));
    }
    let mut records = events
        .into_iter()
        .enumerate()
        .map(|(i, event)| {
            let payload = encoder.encode(LedgerPayload::DomainEvent(event));
            Ok(ProposedRecord {
                schema_version: DottedKey::new(RECORD_SCHEMA_VERSION)
                    .map_err(|_| Error::Corrupt)?,
                record_id: LedgerRecordId::from_uuid(uuid::Uuid::new_v5(
                    &command,
                    &(i as u64).to_be_bytes(),
                ))
                .map_err(|_| Error::Corrupt)?,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(authority.activity_id),
                run_id: Some(run_id),
                run_seq: Some(
                    RunSeq::new(
                        authority
                            .last_run_sequence
                            .get()
                            .checked_add(i as u64 + 1)
                            .ok_or(Error::Corrupt)?,
                    )
                    .map_err(|_| Error::Corrupt)?,
                ),
                attempt_id: None,
                participant_id: Some(authority.requested_by_participant_id),
                producer: Producer::RuntimeApi,
                occurred_at: now,
                causation_id: None,
                correlation_id: correlation,
                payload_hash: Sha256::digest(&payload),
                payload,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    for target in &authority.descendants {
        let reason = CancelReason::ParentCancelled;
        let mut events = Vec::new();
        if let Some(interaction_id) = target.pending_interaction_id {
            events.push(DomainEvent::Interaction(
                musteragent_approval::InteractionEvent::Cancelled {
                    interaction_id,
                    reason,
                },
            ));
        }
        if let Some(invocation_id) = target.pending_tool_invocation_id {
            events.push(DomainEvent::Tool(
                musteragent_tool::ToolEvent::InvocationCancelled {
                    invocation_id,
                    reason,
                },
            ));
        }
        events.push(DomainEvent::Run(
            musteragent_run::RunEvent::CancelRequested {
                reason,
                requested_by: actor.principal_id,
            },
        ));
        for (index, event) in events.into_iter().enumerate() {
            let payload = encoder.encode(LedgerPayload::DomainEvent(event));
            records.push(ProposedRecord {
                schema_version: DottedKey::new(RECORD_SCHEMA_VERSION)
                    .map_err(|_| Error::Corrupt)?,
                record_id: LedgerRecordId::from_uuid(uuid::Uuid::new_v5(
                    &command,
                    format!("{}:{index}", target.run_id).as_bytes(),
                ))
                .map_err(|_| Error::Corrupt)?,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(target.activity_id),
                run_id: Some(target.run_id),
                run_seq: Some(
                    RunSeq::new(
                        target
                            .expected_last_run_sequence
                            .get()
                            .checked_add(index as u64 + 1)
                            .ok_or(Error::Corrupt)?,
                    )
                    .map_err(|_| Error::Corrupt)?,
                ),
                attempt_id: None,
                participant_id: Some(authority.requested_by_participant_id),
                producer: Producer::RuntimeApi,
                occurred_at: now,
                causation_id: None,
                correlation_id: correlation,
                payload_hash: Sha256::digest(&payload),
                payload,
            });
        }
    }
    let batch = CancelRunBatch {
        append: AppendRequest {
            tenant_id: actor.tenant_id,
            conversation_id: authority.conversation_id,
            expected_home_epoch: authority.home_epoch,
            expected_offset: authority.current_offset,
            idempotency_key: IdempotencyKey::new(format!("orchestration-stop:{command}"))
                .map_err(|_| Error::Corrupt)?,
            command_fingerprint: Sha256::digest(command.as_bytes()),
            records,
            outbox: Vec::new(),
        },
        run_id,
        activity_id: authority.activity_id,
        expected_last_run_sequence: authority.last_run_sequence,
        mode: authority.mode,
        reason,
        pending_interaction_id: authority.pending_interaction_id,
        pending_tool_invocation_id: authority.pending_tool_invocation_id,
        unresolved_tool_invocation_id: authority.unresolved_tool_invocation_id,
        descendants: authority.descendants,
    };
    store
        .cancel_run(correlation, batch)
        .await
        .map_err(|_| Error::Unavailable)?;
    Ok(())
}
