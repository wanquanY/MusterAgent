use super::*;
use musteragent_activity::{Activity, ActivityEvent, ActivityKind, ActivityStatus};
use musteragent_conversation::{AdmissionMessage, ConversationEvent, MessageRole};
use musteragent_ledger::*;
use musteragent_runtime_admission::AdmissionIdentities;

pub(crate) fn schedule_batch(
    source: &Source,
    occurrence: uuid::Uuid,
    input: ContentRef,
    now: UtcTimestamp,
    codec: &dyn LedgerPayloadEncoder,
    retry: musteragent_runtime_automation::schedule_policy::RetrySpec,
) -> Result<AdmissionBatch, Error> {
    let ids =
        AdmissionIdentities::derive(CommandId::from_uuid(occurrence).map_err(|_| Error::Invalid)?)
            .ok_or(Error::Invalid)?;
    let context = RunContext {
        tenant_id: source.actor.tenant_id,
        conversation_id: ConversationId::from_uuid(uuid::Uuid::new_v5(
            &occurrence,
            b"schedule.session",
        ))
        .map_err(|_| Error::Corrupt)?,
        activity_id: ids.activity_id,
        activity_kind: ActivityKind::Automation,
        run_id: ids.run_id,
        executor_participant_id: ParticipantId::from_uuid(uuid::Uuid::new_v5(
            &occurrence,
            b"executor",
        ))
        .map_err(|_| Error::Corrupt)?,
        requested_by_participant_id: ParticipantId::from_uuid(uuid::Uuid::new_v5(
            &occurrence,
            b"owner",
        ))
        .map_err(|_| Error::Corrupt)?,
        parent_run_id: None,
        delegation_id: None,
        execution_scope: match source.workspace {
            Some(workspace_id) => ExecutionScope::Workspace { workspace_id },
            None => ExecutionScope::Tenant {
                tenant_id: source.actor.tenant_id,
            },
        },
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch: HomeEpoch::new(1).map_err(|_| Error::Corrupt)?,
        runtime_snapshot_id: ids.runtime_snapshot_id,
    };
    let snapshot = super::snapshot::derive(source, context, now, retry)?;
    let message = AdmissionMessage {
        project_task_target: None,
        message_id: MessageId::from_uuid(uuid::Uuid::new_v5(&occurrence, b"input"))
            .map_err(|_| Error::Corrupt)?,
        participant_id: context.requested_by_participant_id,
        role: MessageRole::User,
        source_run_id: context.run_id,
        content: input,
    };
    let policy =
        VersionedKey::new("musteragent.scheduled-execution@1").map_err(|_| Error::Corrupt)?;
    let activity = Activity {
        activity_id: context.activity_id,
        conversation_id: context.conversation_id,
        kind: context.activity_kind,
        requested_by_participant_id: context.requested_by_participant_id,
        parent_activity_id: None,
        root_activity_id: context.activity_id,
        status: ActivityStatus::Active,
        policy_snapshot_ref: policy.clone(),
        created_at: now,
        completed_at: None,
    };
    let correlation = CorrelationId::from_uuid(occurrence).map_err(|_| Error::Corrupt)?;
    let payloads = [
        (
            ids.message_record_id,
            None,
            Some(context.requested_by_participant_id),
            DomainEvent::Conversation(ConversationEvent::MessageAccepted {
                message_id: message.message_id,
                role: message.role,
                content_ref: message.content.clone(),
                attachment_refs: Vec::new(),
                requested_skill_refs: Vec::new(),
                project_task_target: None,
            }),
        ),
        (
            ids.activity_record_id,
            None,
            None,
            DomainEvent::Activity(ActivityEvent::Started {
                kind: activity.kind,
                root_activity_id: activity.root_activity_id,
                parent_activity_id: None,
                policy_snapshot_ref: policy,
            }),
        ),
        (
            ids.run_record_id,
            Some(context.run_id),
            Some(context.executor_participant_id),
            DomainEvent::Run(RunEvent::Admitted {
                runtime_snapshot_id: context.runtime_snapshot_id,
                execution_home: RuntimeHome::Cloud,
                parent_run_id: None,
                delegation_id: None,
                retry_source_message_id: None,
                retry_source_attempt_number: None,
            }),
        ),
    ];
    let records = payloads
        .into_iter()
        .map(|(record_id, run_id, participant_id, event)| {
            let payload = codec.encode(LedgerPayload::DomainEvent(event));
            Ok(ProposedRecord {
                schema_version: DottedKey::new(RECORD_SCHEMA_VERSION)
                    .map_err(|_| Error::Corrupt)?,
                record_id,
                record_kind: LedgerRecordKind::DomainEvent,
                activity_id: Some(context.activity_id),
                run_id,
                run_seq: run_id
                    .map(|_| RunSeq::new(1))
                    .transpose()
                    .map_err(|_| Error::Corrupt)?,
                attempt_id: None,
                participant_id,
                producer: Producer::RuntimeApi,
                occurred_at: now,
                causation_id: None,
                correlation_id: correlation,
                payload_hash: Sha256::digest(&payload),
                payload,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let dispatch = context.run_id.to_string().into_bytes();
    Ok(AdmissionBatch {
        actor: source.actor,
        trace_context: source.trace.clone(),
        append: AppendRequest {
            tenant_id: context.tenant_id,
            conversation_id: context.conversation_id,
            expected_home_epoch: context.home_epoch,
            expected_offset: LedgerOffset::ZERO,
            idempotency_key: IdempotencyKey::new(format!("schedule:{occurrence}"))
                .map_err(|_| Error::Corrupt)?,
            command_fingerprint: Sha256::digest(occurrence.as_bytes()),
            records,
            outbox: vec![ProposedOutbox {
                message_id: ids.dispatch_id,
                topic: DottedKey::new("run.dispatch").map_err(|_| Error::Corrupt)?,
                payload_hash: Sha256::digest(&dispatch),
                payload: dispatch,
            }],
        },
        message_record_id: ids.message_record_id,
        conversation_title: None,
        activity_record_id: ids.activity_record_id,
        run_record_id: ids.run_record_id,
        message,
        activity,
        run: Run {
            context,
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
