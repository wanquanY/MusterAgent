use super::*;
use musteragent_runtime_session::WorkItemAuthorityRecord;

pub(super) async fn reserve(
    tx: &mut PgConnection,
    c: &CommitWorkItemStart,
    input: &WorkItemStartInput,
    destination: &WorkItemExecutionTarget,
) -> Result<WorkItemExecutionRequest, DialogueStoreError> {
    let s = &c.source;
    let target = input.executing_agent_id.unwrap_or(s.scope.agent_id);
    preparation::lock_agents(tx, s.actor, s.scope, target).await?;
    if target != s.scope.agent_id {
        let member: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent.owned_agent_scenes requester
            JOIN agent.owned_agent_scenes executor USING(primary_agent_id,space_id)
            WHERE requester.agent_id=$1 AND executor.agent_id=$2 AND requester.space_id=$3)",
        )
        .bind(s.scope.agent_id.into_uuid())
        .bind(target.into_uuid())
        .bind(s.scope.space_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if !member {
            return Err(DialogueStoreError::Unavailable);
        }
    }
    lock_ready_content_reference(tx, s.actor.tenant_id, &c.invocation.request_ref).await?;
    let receipt = crate::work_item_authority::commands::create_in_transaction(
        tx,
        s.create(input, c.goal.clone(), c.committed_at),
    )
    .await
    .map_err(authority_error)?;
    if receipt.replayed {
        return Err(DialogueStoreError::IdempotencyConflict);
    }
    let request = reserve_existing(
        tx,
        receipt.record,
        s,
        target,
        c.partner_input.as_ref(),
        c.committed_at,
    )
    .await?;
    device_target::persist(tx, request, destination).await?;
    Ok(request)
}
pub(super) async fn reserve_existing(
    tx: &mut PgConnection,
    record: WorkItemAuthorityRecord,
    s: &WorkItemStartSource,
    target: AgentId,
    partner_input: Option<&musteragent_content_ref::ContentRef>,
    at: UtcTimestamp,
) -> Result<WorkItemExecutionRequest, DialogueStoreError> {
    reserve_assignment(tx, record, s, target, partner_input, at, false).await
}
pub(super) async fn reserve_assignment(
    tx: &mut PgConnection,
    mut record: WorkItemAuthorityRecord,
    s: &WorkItemStartSource,
    target: AgentId,
    partner_input: Option<&musteragent_content_ref::ContentRef>,
    at: UtcTimestamp,
    feedback: bool,
) -> Result<WorkItemExecutionRequest, DialogueStoreError> {
    preparation::lock_agents(tx, s.actor, s.scope, target).await?;
    // Amendment preserves the originally selected Device. It cannot silently
    // fall back to Cloud; fresh membership/consent is rechecked at pickup.
    let previous_device=sqlx::query("SELECT d.* FROM collaboration.work_item_device_targets d JOIN collaboration.work_item_execution_reservations x USING(work_item_id,goal_revision,control_revision,assignment_epoch) WHERE x.work_item_id=$1 AND x.tenant_id=$2 AND x.owner_id=$3 ORDER BY x.assignment_epoch DESC LIMIT 1")
        .bind(record.core.work_item_id.into_uuid()).bind(s.actor.tenant_id.into_uuid()).bind(s.actor.principal_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.as_ref().map(device_target::decode).transpose()?;
    let epoch = AssignmentEpoch::new(
        record
            .core
            .assignment_epoch
            .map_or(1, |value| value.get().saturating_add(1)),
    )
    .map_err(|_| DialogueStoreError::Corrupt)?;
    record.core = if feedback {
        record.core.reserve_returned_execution(
            record.core.revisions.goal,
            record.core.revisions.control,
            epoch,
        )
    } else {
        record.core.reserve_execution(
            record.core.revisions.goal,
            record.core.revisions.control,
            epoch,
        )
    }
    .map_err(|_| DialogueStoreError::RevisionConflict)?;
    record.updated_at = at;
    crate::work_item_authority::commands::revision_and_outbox(tx, &record, "execution_requested")
        .await
        .map_err(authority_error)?;
    sqlx::query("UPDATE collaboration.work_items SET state_revision=$2 WHERE work_item_id=$1")
        .bind(record.core.work_item_id.into_uuid())
        .bind(integer(record.core.revisions.state.get())?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    let row=sqlx::query("SELECT d.dialogue_id,d.epoch,d.head_seq FROM agent.agent_dialogues d WHERE d.tenant_id=$1 AND d.owner_id=$2 AND d.agent_id=$3")
 .bind(s.actor.tenant_id.into_uuid()).bind(s.actor.principal_id.into_uuid()).bind(target.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
    let dialogue = AgentDialogueId::from_uuid(row.try_get("dialogue_id").map_err(db)?)
        .map_err(|_| DialogueStoreError::Corrupt)?;
    let key = WorkItemExecutionKey {
        actor: s.actor,
        scope: s.scope,
        work_item_id: record.core.work_item_id,
        goal_revision: record.core.revisions.goal,
        control_revision: record.core.revisions.control,
        assignment_epoch: epoch,
        report_from_run_id: None,
        executing_agent_id: target,
    };
    let input_entry = if let Some(reference) = partner_input {
        Some(
            crate::dialogue_publication::work_item_input::append(tx, key, dialogue, reference, at)
                .await?,
        )
    } else {
        None
    };
    let sequence = unsigned(row.try_get("head_seq").map_err(db)?)?
        .checked_add(u64::from(input_entry.is_some()))
        .ok_or(DialogueStoreError::Corrupt)?;
    let cut = crate::dialogue_context::load_cut(
        tx,
        s.actor,
        target,
        dialogue,
        DialogueCursor {
            epoch: unsigned(row.try_get("epoch").map_err(db)?)?,
            seq: sequence,
        },
    )
    .await?;
    let ids = key.identities();
    let inputs = inputs::source(
        tx,
        s.actor,
        s.source_run_id,
        s.scope.agent_id,
        s.source_intent_id,
    )
    .await?;
    let (protocol, requester_uses_result) = result_protocol(tx, s).await?;
    sqlx::query("INSERT INTO collaboration.work_item_execution_reservations (work_item_id,goal_revision,control_revision,assignment_epoch,accepted_state_revision,tenant_id,owner_id,space_id,requesting_agent_id,executing_agent_id,executing_dialogue_id,source_run_id,source_invocation_id,source_snapshot_id,context_epoch,context_seq,context_chain_sha256,accepted_at_ms,command_id,conversation_id,run_id,partner_input_entry_id,partner_input_ref,delegated_content_refs,source_home,result_protocol,requester_uses_result) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,CASE WHEN EXISTS(SELECT 1 FROM collaboration.work_item_native_sources n WHERE n.invocation_id=$13) THEN 'device' ELSE 'cloud' END,$25,$26)")
 .bind(key.work_item_id.into_uuid()).bind(integer(key.goal_revision.get())?).bind(integer(key.control_revision.get())?).bind(integer(epoch.get())?).bind(integer(record.core.revisions.state.get())?)
 .bind(s.actor.tenant_id.into_uuid()).bind(s.actor.principal_id.into_uuid()).bind(s.scope.space_id.into_uuid()).bind(s.scope.agent_id.into_uuid()).bind(target.into_uuid()).bind(dialogue.into_uuid())
 .bind(s.source_run_id.into_uuid()).bind(s.source_invocation_id.into_uuid()).bind(s.source_snapshot_id.into_uuid()).bind(integer(cut.through.epoch)?).bind(integer(cut.through.seq)?).bind(cut.source_chain_sha256.into_bytes().to_vec())
 .bind(at.unix_millis()).bind(ids.command_id.into_uuid()).bind(ids.conversation_id.into_uuid()).bind(ids.run_id()?.into_uuid()).bind(input_entry.map(DialogueEntryId::into_uuid)).bind(partner_input.map(content_bytes)).bind(inputs.iter().map(|attachment|content_bytes(&attachment.content_ref)).collect::<Vec<_>>()).bind(protocol).bind(requester_uses_result).execute(&mut *tx).await.map_err(db)?;
    if let Some(target) = previous_device {
        device_target::persist(
            tx,
            key.request(),
            &WorkItemExecutionTarget::RemoteDevice(target),
        )
        .await?;
    }
    Ok(key.request())
}

async fn result_protocol(
    tx: &mut PgConnection,
    s: &WorkItemStartSource,
) -> Result<(&'static str, bool), DialogueStoreError> {
    use musteragent_context::RuntimeSnapshotDecoder;
    let release = if let Some(source) =
        native_source::load(tx, s.actor, s.source_invocation_id).await?
    {
        if &source.source != s {
            return Err(DialogueStoreError::Corrupt);
        }
        source.claims.product_release
    } else {
        let row=sqlx::query("SELECT snap.encoded_snapshot,snap.content_hash FROM capability.runtime_snapshots snap
            JOIN runtime.runs r ON r.tenant_id=snap.tenant_id AND r.run_id=snap.run_id AND r.runtime_snapshot_id=snap.runtime_snapshot_id
            JOIN tool.tool_invocations i ON i.run_id=r.run_id AND i.tenant_id=r.tenant_id AND i.conversation_id=r.conversation_id
            WHERE snap.tenant_id=$1 AND snap.runtime_snapshot_id=$2 AND r.run_id=$3 AND i.invocation_id=$4")
            .bind(s.actor.tenant_id.into_uuid()).bind(s.source_snapshot_id.into_uuid()).bind(s.source_run_id.into_uuid()).bind(s.source_invocation_id.into_uuid())
            .fetch_optional(tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
        let bytes: Vec<u8> = row.try_get("encoded_snapshot").map_err(db)?;
        if musteragent_content_ref::Sha256::digest(&bytes).into_bytes()
            != row.try_get::<Vec<u8>, _>("content_hash").map_err(db)?[..]
        {
            return Err(DialogueStoreError::Corrupt);
        }
        let snapshot = musteragent_persistence_codec::ProtobufSnapshotCodec
            .decode_runtime_snapshot(&bytes)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        snapshot
            .validate_sealed()
            .map_err(|_| DialogueStoreError::Corrupt)?;
        snapshot.product_release
    };
    // Pre-release historical snapshots have no ProductRelease identity.
    let Some(release) = release else {
        return Ok(("public_result_v1", false));
    };
    use musteragent_tool::TaskResultProtocol;
    Ok(
        match musteragent_tool::task_result_protocol(&release)
            .map_err(|_| DialogueStoreError::Unavailable)?
        {
            TaskResultProtocol::Public => ("public_result_v1", false),
            TaskResultProtocol::Verified => ("verified_result_v1", false),
            TaskResultProtocol::Requester => ("verified_result_v1", true),
        },
    )
}
