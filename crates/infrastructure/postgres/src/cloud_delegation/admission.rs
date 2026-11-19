//! Restored private child admission shape from the application delegation owner.
use super::*;
use musteragent_activity::{Activity, ActivityEvent, ActivityKind, ActivityStatus};
use musteragent_conversation::{
    ConversationEvent, Participant, ParticipantDisplaySnapshot, ParticipantKind,
};
use musteragent_event_envelope::*;
use musteragent_ledger::*;
use musteragent_run::{Run, RunState};
use musteragent_runtime_contract::DisplayText;

pub(super) fn batch(
    source: &crate::cloud_admission::Source,
    command: &ChildAdmissionIdentity,
    state: crate::commit_lane::CommitLaneState,
    root: ActivityId,
    objective: ContentRef,
    codec: &dyn LedgerPayloadEncoder,
) -> Result<DelegationAdmissionBatch, Error> {
    let seed = command.invocation_id.into_uuid();
    let id = |tag: &[u8]| uuid::Uuid::new_v5(&seed, tag);
    let ids = musteragent_runtime_admission::AdmissionIdentities::derive(
        CommandId::from_uuid(id(b"delegation-command")).map_err(|_| Error::Corrupt)?,
    )
    .ok_or(Error::Corrupt)?;
    let delegation_id = DelegationId::from_uuid(id(b"delegation")).map_err(|_| Error::Corrupt)?;
    let participant_id =
        ParticipantId::from_uuid(id(b"delegation-participant")).map_err(|_| Error::Corrupt)?;
    let now = command.requested_at;
    let run_context = musteragent_run::RunContext {
        activity_id: ids.activity_id,
        activity_kind: ActivityKind::Delegation,
        run_id: ids.run_id,
        executor_participant_id: participant_id,
        parent_run_id: Some(command.run_context.run_id),
        delegation_id: Some(delegation_id),
        runtime_snapshot_id: ids.runtime_snapshot_id,
        ..command.run_context
    };
    let snapshot =
        crate::cloud_admission::derive_child(source, run_context, now).map_err(source_error)?;
    let participant = Participant {
        participant_id,
        conversation_id: state.conversation_id,
        kind: ParticipantKind::Agent,
        principal_ref: None,
        agent_ref: Some(source.snapshot.agent.agent_id),
        service_ref: None,
        display_snapshot: ParticipantDisplaySnapshot {
            display_name: DisplayText::new("Subagent").map_err(|_| Error::Corrupt)?,
            avatar_ref: None,
        },
        joined_at: now,
        left_at: None,
    };
    let activity = Activity {
        activity_id: ids.activity_id,
        conversation_id: state.conversation_id,
        kind: ActivityKind::Delegation,
        requested_by_participant_id: run_context.requested_by_participant_id,
        parent_activity_id: Some(state.activity_id),
        root_activity_id: root,
        status: ActivityStatus::Active,
        policy_snapshot_ref: source.snapshot.policy.policy_ref.clone(),
        created_at: now,
        completed_at: None,
    };
    let participant_record_id =
        LedgerRecordId::from_uuid(id(b"participant-record")).map_err(|_| Error::Corrupt)?;
    let events = [
        (
            participant_record_id,
            None,
            None,
            Some(participant_id),
            DomainEvent::Conversation(ConversationEvent::ParticipantJoined {
                kind: ParticipantKind::Agent,
                principal_ref: None,
                agent_ref: participant.agent_ref,
                service_ref: None,
                display_snapshot: participant.display_snapshot.clone(),
            }),
        ),
        (
            ids.message_record_id,
            Some(ids.activity_id),
            None,
            Some(run_context.requested_by_participant_id),
            DomainEvent::Activity(ActivityEvent::DelegationContextProjected {
                delegation_id,
                parent_invocation_id: command.invocation_id,
                objective_ref: objective.clone(),
            }),
        ),
        (
            ids.activity_record_id,
            Some(ids.activity_id),
            None,
            None,
            DomainEvent::Activity(ActivityEvent::Started {
                kind: ActivityKind::Delegation,
                root_activity_id: root,
                parent_activity_id: Some(state.activity_id),
                policy_snapshot_ref: activity.policy_snapshot_ref.clone(),
            }),
        ),
        (
            ids.run_record_id,
            Some(ids.activity_id),
            Some(ids.run_id),
            Some(participant_id),
            DomainEvent::Run(musteragent_run::RunEvent::Admitted {
                runtime_snapshot_id: ids.runtime_snapshot_id,
                execution_home: RuntimeHome::Cloud,
                parent_run_id: Some(command.run_context.run_id),
                delegation_id: Some(delegation_id),
                input_message_id: None,
                retry_source_message_id: None,
                retry_source_attempt_number: None,
            }),
        ),
    ];
    let records = events
        .into_iter()
        .map(|(record_id, activity_id, run_id, participant_id, event)| {
            let payload = codec.encode(LedgerPayload::DomainEvent(event));
            Ok(ProposedRecord {
                schema_version: DottedKey::new(RECORD_SCHEMA_VERSION)
                    .map_err(|_| Error::Corrupt)?,
                record_id,
                activity_id,
                run_id,
                participant_id,
                record_kind: LedgerRecordKind::DomainEvent,
                run_seq: run_id
                    .map(|_| RunSeq::new(1))
                    .transpose()
                    .map_err(|_| Error::Corrupt)?,
                attempt_id: None,
                producer: Producer::RuntimeApi,
                occurred_at: now,
                causation_id: None,
                correlation_id: CorrelationId::from_uuid(seed).map_err(|_| Error::Corrupt)?,
                payload_hash: Sha256::digest(&payload),
                payload,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let payload = ids.run_id.to_string().into_bytes();
    let mut append = state
        .append(format!("delegation:{seed}"), records)
        .map_err(|_| Error::Corrupt)?;
    append.outbox.push(ProposedOutbox {
        message_id: ids.dispatch_id,
        topic: DottedKey::new("run.dispatch").map_err(|_| Error::Corrupt)?,
        payload_hash: Sha256::digest(&payload),
        payload,
    });
    Ok(DelegationAdmissionBatch {
        actor: command.actor,
        trace_context: source.trace.clone(),
        append,
        participant_record_id,
        objective_record_id: ids.message_record_id,
        activity_record_id: ids.activity_record_id,
        run_record_id: ids.run_record_id,
        delegation_id,
        objective_ref: objective,
        executor_participant: participant,
        activity,
        run: Run {
            context: run_context,
            state: RunState::queued(),
            next_attempt_number: AttemptNumber::new(1).map_err(|_| Error::Corrupt)?,
            not_before: None,
            cancel_requested_at: None,
            admitted_at: now,
            started_at: None,
            finished_at: None,
            terminal_error: None,
        },
        runtime_snapshot: snapshot,
    })
}
