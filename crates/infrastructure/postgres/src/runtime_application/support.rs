use super::*;

pub(super) fn decode_archive_target(
    row: PgRow,
    correlation_id: CorrelationId,
) -> Result<ArchiveRunTarget, ReadError> {
    let status: i16 = row
        .try_get("status")
        .map_err(|_| ReadError::Dependency(correlation_id))?;
    let has_lease: bool = row
        .try_get("has_lease")
        .map_err(|_| ReadError::Dependency(correlation_id))?;
    let mode = match (status, has_lease) {
        (2, true) | (4, _) => CancelMode::RequestActiveAttempt,
        (1 | 3, false) => CancelMode::CancelWithoutAttempt,
        _ => return Err(ReadError::Corrupt(correlation_id)),
    };
    let pending_interaction_id = optional_id(
        row.try_get("wait_interaction_id")
            .map_err(|_| ReadError::Dependency(correlation_id))?,
        correlation_id,
    )?;
    let pending_tool_invocation_id = optional_id(
        row.try_get("pending_tool_invocation_id")
            .map_err(|_| ReadError::Dependency(correlation_id))?,
        correlation_id,
    )?;
    let wait_reason: Option<i16> = row
        .try_get("wait_reason")
        .map_err(|_| ReadError::Dependency(correlation_id))?;
    if invalid_wait_shape(
        mode,
        wait_reason,
        pending_interaction_id.is_some(),
        pending_tool_invocation_id.is_some(),
    ) {
        return Err(ReadError::Corrupt(correlation_id));
    }
    Ok(ArchiveRunTarget {
        run_id: id(
            row.try_get("run_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        activity_id: id(
            row.try_get("activity_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        last_run_sequence: musteragent_identity::RunSeq::new(parse_u64(
            row.try_get("last_run_sequence")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?)
        .map_err(|_| ReadError::Corrupt(correlation_id))?,
        last_attempt_id: optional_id(
            row.try_get("last_attempt_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        mode,
        pending_interaction_id,
        pending_tool_invocation_id,
        unresolved_tool_invocation_id: optional_id(
            row.try_get("unresolved_tool_invocation_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
    })
}

pub(super) const fn archive_target_record_count(target: &ArchiveRunTarget) -> usize {
    match (
        target.mode,
        target.pending_interaction_id.is_some(),
        target.pending_tool_invocation_id.is_some(),
    ) {
        (CancelMode::RequestActiveAttempt, false, false) => 1,
        (CancelMode::CancelWithoutAttempt, false, false) => 3,
        (CancelMode::CancelWithoutAttempt, false, true) => 4,
        (CancelMode::CancelWithoutAttempt, true, true) => 5,
        _ => 0,
    }
}

pub(super) fn validate_archive(commit: &ArchiveConversationCommit) -> Result<(), ErrorCode> {
    if commit.actor.tenant_id != commit.append.tenant_id
        || !commit.append.outbox.is_empty()
        || commit.append.records.len()
            != commit
                .targets
                .iter()
                .map(archive_target_record_count)
                .sum::<usize>()
                .saturating_add(1)
    {
        return Err(ErrorCode::InvalidCommand);
    }
    let target_ids = commit
        .targets
        .iter()
        .map(|target| target.run_id)
        .collect::<BTreeSet<_>>();
    if target_ids.len() != commit.targets.len() {
        return Err(ErrorCode::InvalidCommand);
    }
    for target in &commit.targets {
        if target.mode == CancelMode::QueueCancellation
            || (target.unresolved_tool_invocation_id.is_some()
                && (target.pending_tool_invocation_id.is_some()
                    || target.pending_interaction_id.is_some()
                    || target.mode == CancelMode::RequestActiveAttempt))
            || (target.pending_interaction_id.is_some()
                && target.pending_tool_invocation_id.is_none())
            || (target.mode == CancelMode::RequestActiveAttempt
                && (target.pending_interaction_id.is_some()
                    || target.pending_tool_invocation_id.is_some()))
        {
            return Err(ErrorCode::InvalidCommand);
        }
        let records = commit
            .append
            .records
            .iter()
            .filter(|record| record.run_id == Some(target.run_id))
            .collect::<Vec<_>>();
        if records.len() != archive_target_record_count(target) {
            return Err(ErrorCode::InvalidCommand);
        }
        for (index, record) in records.into_iter().enumerate() {
            let increment =
                u64::try_from(index + 1).map_err(|_| ErrorCode::InternalInvariantViolated)?;
            let expected_run_sequence = target
                .last_run_sequence
                .get()
                .checked_add(increment)
                .and_then(|value| musteragent_identity::RunSeq::new(value).ok())
                .ok_or(ErrorCode::InternalInvariantViolated)?;
            if record.activity_id != Some(target.activity_id)
                || record.run_seq != Some(expected_run_sequence)
                || record.attempt_id.is_some()
                || record.producer != Producer::RuntimeApi
            {
                return Err(ErrorCode::InvalidCommand);
            }
        }
    }
    let conversation_records = commit
        .append
        .records
        .iter()
        .filter(|record| record.run_id.is_none())
        .collect::<Vec<_>>();
    if conversation_records.len() != 1
        || conversation_records[0].activity_id.is_some()
        || conversation_records[0].run_seq.is_some()
        || conversation_records[0].attempt_id.is_some()
        || conversation_records[0].producer != Producer::RuntimeApi
    {
        return Err(ErrorCode::InvalidCommand);
    }
    Ok(())
}

pub(super) const fn invalid_wait_shape(
    mode: CancelMode,
    wait_reason: Option<i16>,
    has_pending_interaction: bool,
    has_pending_tool: bool,
) -> bool {
    (has_pending_interaction && !has_pending_tool)
        || (has_pending_tool && matches!(mode, CancelMode::RequestActiveAttempt))
        || (has_pending_tool
            && !has_pending_interaction
            && !matches!(wait_reason, Some(3) | Some(7)))
}

pub(super) fn validate_create(commit: &CreateConversationCommit) -> Result<(), ErrorCode> {
    let conversation = &commit.conversation;
    if commit.actor.tenant_id != conversation.tenant_id
        || commit.actor.principal_id != conversation.created_by
        || conversation.kind != ConversationKind::Direct
        || conversation.control_home != RuntimeHome::Cloud
        || conversation.lifecycle != ConversationLifecycle::Active
        || commit.append.tenant_id != conversation.tenant_id
        || commit.append.conversation_id != conversation.conversation_id
        || commit.append.expected_offset != LedgerOffset::ZERO
        || commit.append.records.len() != 3
        || !commit.append.outbox.is_empty()
        || commit.user_participant.conversation_id != conversation.conversation_id
        || commit.user_participant.principal_ref != Some(commit.actor.principal_id)
        || commit.user_participant.kind != ParticipantKind::User
        || commit.assistant_participant.conversation_id != conversation.conversation_id
        || commit.assistant_participant.kind != ParticipantKind::Agent
        || commit.assistant_participant.agent_ref.is_none()
        || commit.assistant_participant.participant_id
            != conversation.execution_owner_participant_id
    {
        Err(ErrorCode::InvalidCommand)
    } else {
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) enum ReadError {
    Dependency(CorrelationId),
    Corrupt(CorrelationId),
}

impl From<ReadError> for RuntimeError {
    fn from(value: ReadError) -> Self {
        match value {
            ReadError::Dependency(correlation_id) => dependency(correlation_id),
            ReadError::Corrupt(correlation_id) => corrupt(correlation_id),
        }
    }
}

pub(super) async fn insert_conversation(
    transaction: &mut sqlx::PgConnection,
    commit: &CreateConversationCommit,
) -> Result<(), sqlx::Error> {
    let value = &commit.conversation;
    sqlx::query(
        "INSERT INTO conversation.conversations (
             conversation_id, tenant_id, workspace_id, kind, title, control_home_kind,
             control_device_id, home_epoch, lifecycle, created_by,
             created_at_ms, updated_at_ms, archived_at_ms,
             execution_owner_participant_id
         ) VALUES ($1, $2, $3, 1, $4, 1, NULL, $5::bigint, 1, $6, $7, $7, NULL, $8)",
    )
    .bind(value.conversation_id.into_uuid())
    .bind(value.tenant_id.into_uuid())
    .bind(value.workspace_id.map(|item| item.into_uuid()))
    .bind(value.title.as_ref().map(DisplayText::as_str))
    .bind(value.home_epoch.get().to_string())
    .bind(value.created_by.into_uuid())
    .bind(value.created_at.unix_millis())
    .bind(value.execution_owner_participant_id.into_uuid())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(super) async fn insert_participant(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    value: &Participant,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO conversation.participants (
             participant_id, tenant_id, conversation_id, kind, principal_ref,
             agent_ref, service_ref, display_name, avatar_content_id,
             joined_at_ms, left_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL)",
    )
    .bind(value.participant_id.into_uuid())
    .bind(tenant_id.into_uuid())
    .bind(value.conversation_id.into_uuid())
    .bind(participant_kind_code(value.kind))
    .bind(value.principal_ref.map(PrincipalId::into_uuid))
    .bind(value.agent_ref.map(AgentId::into_uuid))
    .bind(value.service_ref.as_ref().map(ServiceRef::as_str))
    .bind(value.display_snapshot.display_name.as_str())
    .bind(
        value
            .display_snapshot
            .avatar_ref
            .as_ref()
            .map(|content| content.content_id.into_uuid()),
    )
    .bind(value.joined_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(super) async fn verify_create_duplicate(
    pool: &PgPool,
    commit: &CreateConversationCommit,
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError> {
    let matches: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM conversation.conversations AS conversation
             JOIN conversation.participants AS user_participant
               ON user_participant.conversation_id = conversation.conversation_id
              AND user_participant.participant_id = $4
             JOIN conversation.participants AS assistant_participant
               ON assistant_participant.conversation_id = conversation.conversation_id
              AND assistant_participant.participant_id = $5
             WHERE conversation.tenant_id = $1
               AND conversation.conversation_id = $2
               AND conversation.created_by = $3
               AND conversation.workspace_id IS NOT DISTINCT FROM $6
               AND conversation.execution_owner_participant_id = $5
         )",
    )
    .bind(commit.actor.tenant_id.into_uuid())
    .bind(commit.conversation.conversation_id.into_uuid())
    .bind(commit.actor.principal_id.into_uuid())
    .bind(commit.user_participant.participant_id.into_uuid())
    .bind(commit.assistant_participant.participant_id.into_uuid())
    .bind(
        commit
            .conversation
            .workspace_id
            .map(|value| value.into_uuid()),
    )
    .fetch_one(pool)
    .await
    .map_err(|_| dependency(correlation_id))?;
    if matches {
        Ok(())
    } else {
        Err(runtime_error(
            ErrorCode::IdempotencyConflict,
            correlation_id,
            false,
            "Create Conversation idempotency identity conflicts",
        ))
    }
}

pub(super) fn create_ack(
    command_id: CommandId,
    commit: &CreateConversationCommit,
    receipt: musteragent_ledger::AppendReceipt,
) -> CommandAck {
    CommandAck {
        command_id,
        disposition: match receipt.disposition {
            AppendDisposition::Committed => CommandDisposition::Committed,
            AppendDisposition::Duplicate => CommandDisposition::Duplicate,
        },
        committed_at: receipt.committed_at,
        first_conversation_seq: Some(receipt.first_sequence),
        last_conversation_seq: Some(receipt.last_sequence),
        resource_refs: vec![ResourceRef::Conversation(
            commit.conversation.conversation_id,
        )],
    }
}

pub(super) async fn authorize_conversation(
    pool: &PgPool,
    actor: Actor,
    conversation_id: ConversationId,
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError> {
    let authorized: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM conversation.participants AS p
             JOIN conversation.conversations AS c ON c.tenant_id=p.tenant_id AND c.conversation_id=p.conversation_id AND c.purpose='standard'
             WHERE p.tenant_id = $1 AND p.conversation_id = $2
               AND p.kind = 1 AND p.principal_ref = $3 AND p.left_at_ms IS NULL
         )",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_one(pool)
    .await
    .map_err(|_| dependency(correlation_id))?;
    if authorized {
        Ok(())
    } else {
        Err(runtime_error(
            ErrorCode::ParticipantNotAuthorized,
            correlation_id,
            false,
            "Conversation participant is not authorized",
        ))
    }
}

pub(super) fn decode_conversation(
    row: &PgRow,
    correlation_id: CorrelationId,
) -> Result<Conversation, ReadError> {
    let lifecycle = match row
        .try_get::<i16, _>("lifecycle")
        .map_err(|_| ReadError::Dependency(correlation_id))?
    {
        1 => ConversationLifecycle::Active,
        2 => ConversationLifecycle::Archived,
        _ => return Err(ReadError::Corrupt(correlation_id)),
    };
    let kind = match row
        .try_get::<i16, _>("kind")
        .map_err(|_| ReadError::Dependency(correlation_id))?
    {
        1 => ConversationKind::Direct,
        2 => ConversationKind::Team,
        3 => ConversationKind::Automation,
        4 => ConversationKind::System,
        _ => return Err(ReadError::Corrupt(correlation_id)),
    };
    let last_sequence = parse_u64(
        row.try_get("last_sequence")
            .map_err(|_| ReadError::Dependency(correlation_id))?,
        correlation_id,
    )?;
    Ok(Conversation {
        conversation_id: id(
            row.try_get("conversation_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        tenant_id: id(
            row.try_get("tenant_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        workspace_id: row
            .try_get::<Option<uuid::Uuid>, _>("workspace_id")
            .map_err(|_| ReadError::Dependency(correlation_id))?
            .map(|value| id(value, correlation_id))
            .transpose()?,
        kind,
        execution_owner_participant_id: id(
            row.try_get("execution_owner_participant_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        title: row
            .try_get::<Option<String>, _>("title")
            .map_err(|_| ReadError::Dependency(correlation_id))?
            .map(DisplayText::new)
            .transpose()
            .map_err(|_| ReadError::Corrupt(correlation_id))?,
        control_home: RuntimeHome::Cloud,
        home_epoch: HomeEpoch::new(parse_u64(
            row.try_get("home_epoch")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?)
        .map_err(|_| ReadError::Corrupt(correlation_id))?,
        next_conversation_seq: ConversationSeq::new(last_sequence.saturating_add(1))
            .map_err(|_| ReadError::Corrupt(correlation_id))?,
        lifecycle,
        created_by: id(
            row.try_get("created_by")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        created_at: UtcTimestamp::from_unix_millis(
            row.try_get("created_at_ms")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
        ),
        updated_at: UtcTimestamp::from_unix_millis(
            row.try_get("updated_at_ms")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
        ),
        archived_at: row
            .try_get::<Option<i64>, _>("archived_at_ms")
            .map_err(|_| ReadError::Dependency(correlation_id))?
            .map(UtcTimestamp::from_unix_millis),
    })
}

pub(super) async fn load_participants(
    pool: &PgPool,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    correlation_id: CorrelationId,
) -> Result<Vec<Participant>, ReadError> {
    let rows = sqlx::query(
        "SELECT participant_id, conversation_id, kind, principal_ref, agent_ref,
                service_ref, display_name, joined_at_ms, left_at_ms
         FROM conversation.participants
         WHERE tenant_id = $1 AND conversation_id = $2
         ORDER BY joined_at_ms, participant_id",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_all(pool)
    .await
    .map_err(|_| ReadError::Dependency(correlation_id))?;
    rows.into_iter()
        .map(|row| decode_participant(row, correlation_id))
        .collect()
}

pub(super) fn decode_participant(
    row: PgRow,
    correlation_id: CorrelationId,
) -> Result<Participant, ReadError> {
    let kind = match row
        .try_get::<i16, _>("kind")
        .map_err(|_| ReadError::Dependency(correlation_id))?
    {
        1 => ParticipantKind::User,
        2 => ParticipantKind::Agent,
        3 => ParticipantKind::Service,
        _ => return Err(ReadError::Corrupt(correlation_id)),
    };
    let identity = ParticipantIdentity {
        principal_ref: optional_id(
            row.try_get("principal_ref")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        agent_ref: optional_id(
            row.try_get("agent_ref")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        service_ref: row
            .try_get::<Option<String>, _>("service_ref")
            .map_err(|_| ReadError::Dependency(correlation_id))?
            .map(musteragent_identity::DottedKey::new)
            .transpose()
            .map_err(|_| ReadError::Corrupt(correlation_id))?
            .map(ServiceRef::from_key),
    };
    let mut participant = Participant::new(
        id(
            row.try_get("participant_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        id(
            row.try_get("conversation_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        kind,
        identity,
        ParticipantDisplaySnapshot {
            display_name: DisplayText::new(
                row.try_get::<String, _>("display_name")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
            )
            .map_err(|_| ReadError::Corrupt(correlation_id))?,
            avatar_ref: None,
        },
        UtcTimestamp::from_unix_millis(
            row.try_get("joined_at_ms")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
        ),
    )
    .map_err(|_| ReadError::Corrupt(correlation_id))?;
    participant.left_at = row
        .try_get::<Option<i64>, _>("left_at_ms")
        .map_err(|_| ReadError::Dependency(correlation_id))?
        .map(UtcTimestamp::from_unix_millis);
    Ok(participant)
}

pub(super) fn decode_run_view(
    row: PgRow,
    correlation_id: CorrelationId,
) -> Result<RunView, ReadError> {
    let status = run_status(
        row.try_get("status")
            .map_err(|_| ReadError::Dependency(correlation_id))?,
        correlation_id,
    )?;
    let wait_reason = decode_wait_reason(&row, correlation_id)?;
    let active_attempt_id: Option<AttemptId> = optional_id(
        row.try_get("active_attempt_id")
            .map_err(|_| ReadError::Dependency(correlation_id))?,
        correlation_id,
    )?;
    let context = RunContext {
        tenant_id: id(
            row.try_get("tenant_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        conversation_id: id(
            row.try_get("conversation_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        activity_id: id(
            row.try_get("activity_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        activity_kind: activity_kind(
            row.try_get("activity_kind")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        run_id: id(
            row.try_get("run_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        executor_participant_id: id(
            row.try_get("executor_participant_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        requested_by_participant_id: id(
            row.try_get("requested_by_participant_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        parent_run_id: optional_id(
            row.try_get("parent_run_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        delegation_id: optional_id(
            row.try_get("delegation_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
        execution_scope: decode_scope(&row, correlation_id)?,
        control_home: RuntimeHome::Cloud,
        execution_home: RuntimeHome::Cloud,
        home_epoch: HomeEpoch::new(parse_u64(
            row.try_get("home_epoch_text")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?)
        .map_err(|_| ReadError::Corrupt(correlation_id))?,
        runtime_snapshot_id: id(
            row.try_get("runtime_snapshot_id")
                .map_err(|_| ReadError::Dependency(correlation_id))?,
            correlation_id,
        )?,
    };
    let terminal_error = decode_terminal_error(&row, correlation_id)?;
    let run = Run {
        context,
        state: RunState::rehydrate(status, wait_reason, active_attempt_id)
            .map_err(|_| ReadError::Corrupt(correlation_id))?,
        next_attempt_number: AttemptNumber::new(
            u32::try_from(
                row.try_get::<i32, _>("next_attempt_number")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
            )
            .map_err(|_| ReadError::Corrupt(correlation_id))?,
        )
        .map_err(|_| ReadError::Corrupt(correlation_id))?,
        not_before: optional_timestamp(&row, "not_before_ms", correlation_id)?,
        cancel_requested_at: optional_timestamp(&row, "cancel_requested_at_ms", correlation_id)?,
        admitted_at: timestamp(&row, "admitted_at_ms", correlation_id)?,
        started_at: optional_timestamp(&row, "started_at_ms", correlation_id)?,
        finished_at: optional_timestamp(&row, "finished_at_ms", correlation_id)?,
        terminal_error,
    };
    run.validate()
        .map_err(|_| ReadError::Corrupt(correlation_id))?;
    let active_attempt = match active_attempt_id {
        Some(attempt_id) => Some(AttemptContext {
            attempt_id,
            run_id: context.run_id,
            attempt_number: AttemptNumber::new(
                u32::try_from(
                    row.try_get::<i32, _>("attempt_number")
                        .map_err(|_| ReadError::Dependency(correlation_id))?,
                )
                .map_err(|_| ReadError::Corrupt(correlation_id))?,
            )
            .map_err(|_| ReadError::Corrupt(correlation_id))?,
            worker_id: WorkerId::new(
                row.try_get::<String, _>("worker_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
            )
            .map_err(|_| ReadError::Corrupt(correlation_id))?,
            lease_id: id(
                row.try_get("lease_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?,
            fencing_token: FencingToken::new(parse_u64(
                row.try_get("fencing_token")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?)
            .map_err(|_| ReadError::Corrupt(correlation_id))?,
            lease_expires_at: timestamp(&row, "expires_at_ms", correlation_id)?,
        }),
        None => None,
    };
    let offset = LedgerOffset::new(parse_u64(
        row.try_get("conversation_offset")
            .map_err(|_| ReadError::Dependency(correlation_id))?,
        correlation_id,
    )?);
    Ok(RunView {
        input_message_id: None,
        retry_configuration: None,
        run,
        active_attempt,
        pending_interaction_ids: Vec::new(),
        latest_cursor: LedgerCursor {
            conversation_id: context.conversation_id,
            home_epoch: context.home_epoch,
            after_conversation_seq: offset,
        },
    })
}

pub(super) fn decode_wait_reason(
    row: &PgRow,
    correlation_id: CorrelationId,
) -> Result<Option<RunWaitReason>, ReadError> {
    let code: Option<i16> = row
        .try_get("wait_reason")
        .map_err(|_| ReadError::Dependency(correlation_id))?;
    match code {
        None => Ok(None),
        Some(1) => Ok(Some(RunWaitReason::Approval {
            interaction_id: id(
                row.try_get("wait_interaction_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?,
        })),
        Some(2) => Ok(Some(RunWaitReason::Input {
            interaction_id: id(
                row.try_get("wait_interaction_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?,
        })),
        Some(3) => Ok(Some(RunWaitReason::Device {
            invocation_id: id(
                row.try_get("wait_invocation_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?,
        })),
        Some(4) => Ok(Some(RunWaitReason::RetryBackoff)),
        Some(5) => Ok(Some(RunWaitReason::Budget)),
        Some(6) => Ok(Some(RunWaitReason::Delegation {
            child_activity_id: id(
                row.try_get("wait_child_activity_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?,
        })),
        Some(7) => Ok(Some(RunWaitReason::ExternalOperation {
            invocation_id: id(
                row.try_get("wait_invocation_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?,
        })),
        Some(8) => Ok(Some(RunWaitReason::DelegationGroup {
            group_id: id(
                row.try_get("wait_delegation_group_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?,
        })),
        Some(9) => Ok(Some(RunWaitReason::DelegationCompletions)),
        Some(10) => {
            let value: String = row
                .try_get("wait_error_code")
                .map_err(|_| ReadError::Corrupt(correlation_id))?;
            let code =
                ErrorCode::from_safe_code(&value).ok_or(ReadError::Corrupt(correlation_id))?;
            Ok(Some(RunWaitReason::RecoveryRequired { code }))
        }
        Some(11) => Ok(Some(RunWaitReason::ExecutionRecovery)),
        Some(_) => Err(ReadError::Corrupt(correlation_id)),
    }
}

pub(super) fn decode_scope(
    row: &PgRow,
    correlation_id: CorrelationId,
) -> Result<ExecutionScope, ReadError> {
    match row
        .try_get::<i16, _>("execution_scope_kind")
        .map_err(|_| ReadError::Dependency(correlation_id))?
    {
        1 => Ok(ExecutionScope::Tenant {
            tenant_id: id(
                row.try_get("tenant_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?,
        }),
        2 => Ok(ExecutionScope::Workspace {
            workspace_id: id(
                row.try_get("workspace_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?,
        }),
        3 => Ok(ExecutionScope::DevicePrivate {
            device_id: id(
                row.try_get("device_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?,
            workspace_id: optional_id(
                row.try_get("workspace_id")
                    .map_err(|_| ReadError::Dependency(correlation_id))?,
                correlation_id,
            )?,
        }),
        _ => Err(ReadError::Corrupt(correlation_id)),
    }
}

pub(super) fn decode_terminal_error(
    row: &PgRow,
    correlation_id: CorrelationId,
) -> Result<Option<ErrorInfo>, ReadError> {
    let value: Option<Vec<u8>> = row
        .try_get("terminal_error")
        .map_err(|_| ReadError::Dependency(correlation_id))?;
    let Some(value) = value else {
        return Ok(None);
    };
    let code_text = std::str::from_utf8(&value).map_err(|_| ReadError::Corrupt(correlation_id))?;
    let code = ErrorCode::from_safe_code(code_text).ok_or(ReadError::Corrupt(correlation_id))?;
    Ok(Some(ErrorInfo {
        code,
        category: code.category(),
        safe_message: SafeDisplayText::new("Run failed")
            .map_err(|_| ReadError::Corrupt(correlation_id))?,
        retryable: false,
        dependency_ref: None,
        diagnostic_ref: None,
    }))
}

pub(super) fn run_status(
    value: i16,
    correlation_id: CorrelationId,
) -> Result<RunStatus, ReadError> {
    match value {
        1 => Ok(RunStatus::Queued),
        2 => Ok(RunStatus::Running),
        3 => Ok(RunStatus::Waiting),
        4 => Ok(RunStatus::Cancelling),
        5 => Ok(RunStatus::Completed),
        6 => Ok(RunStatus::Failed),
        7 => Ok(RunStatus::Cancelled),
        8 => Ok(RunStatus::Interrupted),
        _ => Err(ReadError::Corrupt(correlation_id)),
    }
}

pub(super) fn activity_kind(
    value: i16,
    correlation_id: CorrelationId,
) -> Result<ActivityKind, ReadError> {
    match value {
        1 => Ok(ActivityKind::Chat),
        2 => Ok(ActivityKind::Coordination),
        3 => Ok(ActivityKind::Automation),
        4 => Ok(ActivityKind::Delegation),
        5 => Ok(ActivityKind::Maintenance),
        6 => Ok(ActivityKind::Evaluation),
        _ => Err(ReadError::Corrupt(correlation_id)),
    }
}

const fn participant_kind_code(value: ParticipantKind) -> i16 {
    match value {
        ParticipantKind::User => 1,
        ParticipantKind::Agent => 2,
        ParticipantKind::Service => 3,
    }
}

pub(super) fn timestamp(
    row: &PgRow,
    field: &str,
    correlation_id: CorrelationId,
) -> Result<UtcTimestamp, ReadError> {
    Ok(UtcTimestamp::from_unix_millis(
        row.try_get(field)
            .map_err(|_| ReadError::Dependency(correlation_id))?,
    ))
}

pub(super) fn optional_timestamp(
    row: &PgRow,
    field: &str,
    correlation_id: CorrelationId,
) -> Result<Option<UtcTimestamp>, ReadError> {
    Ok(row
        .try_get::<Option<i64>, _>(field)
        .map_err(|_| ReadError::Dependency(correlation_id))?
        .map(UtcTimestamp::from_unix_millis))
}

pub(super) fn id<T>(value: uuid::Uuid, correlation_id: CorrelationId) -> Result<T, ReadError>
where
    T: FromUuid,
{
    T::from_uuid(value).map_err(|_| ReadError::Corrupt(correlation_id))
}

pub(super) fn optional_id<T>(
    value: Option<uuid::Uuid>,
    correlation_id: CorrelationId,
) -> Result<Option<T>, ReadError>
where
    T: FromUuid,
{
    value.map(|value| id(value, correlation_id)).transpose()
}

pub(super) trait FromUuid: Sized {
    fn from_uuid(value: uuid::Uuid) -> Result<Self, musteragent_identity::IdentityError>;
}

macro_rules! from_uuid {
    ($($identity:ty),+ $(,)?) => {
        $(impl FromUuid for $identity {
            fn from_uuid(
                value: uuid::Uuid,
            ) -> Result<Self, musteragent_identity::IdentityError> {
                Self::from_uuid(value)
            }
        })+
    };
}

from_uuid!(
    ActivityId,
    AgentId,
    AttemptId,
    ContextBranchId,
    ConversationId,
    DelegationId,
    musteragent_identity::DelegationGroupId,
    DeviceId,
    InteractionId,
    LeaseId,
    LedgerRecordId,
    MessageId,
    ParticipantId,
    PrincipalId,
    RunId,
    RuntimeSnapshotId,
    TenantId,
    WorkspaceId,
    musteragent_identity::InvocationId,
);

pub(super) fn parse_u64(value: String, correlation_id: CorrelationId) -> Result<u64, ReadError> {
    value
        .parse()
        .map_err(|_| ReadError::Corrupt(correlation_id))
}
