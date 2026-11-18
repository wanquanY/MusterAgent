use super::*;
use crate::admission::{self, PostgresAdmissionError};
use crate::admission_refs::RunAdmissionRef;
use crate::ledger::BeginAppend;
use musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION;
fn admission_error(error: PostgresAdmissionError) -> DialogueStoreError {
    tracing::warn!(class=?std::mem::discriminant(&error),"Task original admission owner rejected transaction");
    if let PostgresAdmissionError::Database(database) = error {
        return db(database);
    }
    match error {
        PostgresAdmissionError::ConversationUnavailable => DialogueStoreError::Unavailable,
        PostgresAdmissionError::ContentRefConflict
        | PostgresAdmissionError::SnapshotGrantProjectionMismatch => DialogueStoreError::Corrupt,
        _ => DialogueStoreError::Infrastructure,
    }
}
impl PostgresWorkItemExecutionStore {
    pub(super) async fn commit(
        &self,
        commit: WorkItemExecutionAdmissionCommit,
    ) -> Result<WorkItemExecutionReceipt, DialogueStoreError> {
        commit.validate().inspect_err(|_| {
            tracing::warn!(
                "task.handoff.stage" = "commit_contract",
                "task.handoff.error" = "invalid_contract"
            )
        })?;
        validate_input_record(&commit).inspect_err(|_| {
            tracing::warn!(
                "task.handoff.stage" = "message_record",
                "task.handoff.error" = "invalid_contract"
            )
        })?;
        let p = &commit.preparation;
        if p.target != WorkItemExecutionTarget::Cloud {
            return Err(DialogueStoreError::Unavailable);
        }
        let batch = &commit.admission.batch;
        let mut prepared = match self
            .admission
            .ledger
            .begin_append(&batch.append)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?
        {
            BeginAppend::Duplicate(_) => {
                self.admission
                    .validate_duplicate_work_item_admission(&commit)
                    .await
                    .map_err(admission_error)?;
                let receipt = self
                    .lookup(p.key.request())
                    .await?
                    .ok_or(DialogueStoreError::Corrupt)?;
                if receipt.request != p.key.request()
                    || receipt.run_id != p.identities().run_id()?
                    || receipt.conversation_id != p.identities().conversation_id
                {
                    return Err(DialogueStoreError::Corrupt);
                }
                return Ok(receipt);
            }
            BeginAppend::Ready(value) => value,
        };
        let sequence = prepared.sequences()[0];
        let tx = prepared.transaction_mut();
        let current =
            preparation::load(tx, p.key.request(), true, self.admission.snapshots.as_ref()).await?;
        if current != *p {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM collaboration.work_items t JOIN collaboration.work_item_revisions r ON r.work_item_id=t.work_item_id AND r.state_revision=t.state_revision WHERE t.work_item_id=ANY($1) AND (r.status<>'completed' OR t.erasure_job_id IS NOT NULL))")
        .bind(p.work_item.core.spec.dependencies.iter().map(|v|v.into_uuid()).collect::<Vec<_>>()).fetch_one(&mut *tx).await.map_err(db)?;
        if waiting {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let snapshot = self
            .admission
            .validate_work_item_admission_prefix(tx, &commit)
            .await
            .inspect_err(|_| {
                tracing::warn!(
                    "task.handoff.stage" = "admission_prefix",
                    "task.handoff.error" = "invalid_contract"
                )
            })
            .map_err(admission_error)?;
        if !matches!(snapshot.schema_version.as_str(), SNAPSHOT_SCHEMA_VERSION)
            || snapshot.agent.agent_version_id != p.executing_agent.published_version_id
            || snapshot.agent.agent_id != p.executing_agent.agent_id
            || batch.runtime_snapshot.execution_grant.permission_mode
                != Some(musteragent_policy::PermissionMode::ServiceManaged)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        musteragent_runtime_session::validate_work_item_snapshot_source(&snapshot, p)
            .inspect_err(|_| {
                tracing::warn!(
                    "task.handoff.stage" = "snapshot_source",
                    "task.handoff.error" = "invalid_contract"
                )
            })
            .map_err(|_| DialogueStoreError::Corrupt)?;
        insert_context(tx, p, batch.run.admitted_at)
            .await
            .inspect_err(|_e| {
                tracing::warn!(report = p.report.is_some(), "Task session insertion failed");
            })?;
        self.admission
            .publish_content_stage(tx, &commit.admission.content_stage)
            .await
            .map_err(admission_error)?;
        admission::authorize_content(tx, batch)
            .await
            .map_err(admission_error)?;
        admission::insert_message(tx, batch, sequence.get())
            .await
            .map_err(admission_error)?;
        admission::insert_activity(tx, batch)
            .await
            .map_err(admission_error)?;
        admission::consume_execution_grant(tx, batch)
            .await
            .map_err(admission_error)?;
        admission::insert_execution_grant_routes(tx, batch)
            .await
            .map_err(admission_error)?;
        admission::persist_interactive_run_projection(
            tx,
            RunAdmissionRef::from(batch),
            sequence.get(),
            &snapshot,
        )
        .await
        .map_err(admission_error)?;
        crate::dialogue_execution::commit::retain_snapshot_sources(
            tx,
            p.key.actor.tenant_id,
            batch.runtime_snapshot.runtime_snapshot_id,
            &snapshot,
        )
        .await?;
        if let Some(report) = &p.report {
            sqlx::query("UPDATE agent.owned_agent_inbox_entries SET consumed_at_ms=$2 WHERE report_from_run_id=$1 AND agent_id=$3 AND tenant_id=$4 AND owner_id=$5 AND consumed_at_ms IS NULL")
                .bind(report.producing_run_id.into_uuid()).bind(batch.run.admitted_at.unix_millis()).bind(p.key.scope.agent_id.into_uuid()).bind(p.key.actor.tenant_id.into_uuid()).bind(p.key.scope.owner_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
        } else {
            let mut updated = p.work_item.clone();
            updated.core = updated
                .core
                .execution_admitted(p.binding()?)
                .map_err(|_| DialogueStoreError::RevisionConflict)?;
            updated.updated_at = batch.run.admitted_at;
            crate::work_item_authority::commands::revision_and_outbox(
                tx,
                &updated,
                "execution_admitted",
            )
            .await
            .map_err(authority_error)?;
            sqlx::query(
                "UPDATE collaboration.work_items SET state_revision=$2 WHERE work_item_id=$1",
            )
            .bind(p.key.work_item_id.into_uuid())
            .bind(integer(updated.core.revisions.state.get())?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        let mut staged = prepared
            .stage(&batch.append)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        self.admission
            .audit
            .append_admission_allowed_in_transaction(
                staged.transaction_mut(),
                p.key.actor.tenant_id,
                batch.run.context.run_id,
            )
            .await
            .map_err(|error| {tracing::warn!(class=?std::mem::discriminant(&error),"Task canonical admission audit failed");DialogueStoreError::Infrastructure})?;
        staged.commit().await.map_err(|error| {
            tracing::warn!(class=?std::mem::discriminant(&error),"Task admission commit failed");
            match error {
                crate::PostgresLedgerError::Database(error) => db(error),
                crate::PostgresLedgerError::Transaction(
                    crate::PostgresTransactionError::Database(error),
                ) => db(error),
                _ => DialogueStoreError::Infrastructure,
            }
        })?;
        Ok(WorkItemExecutionReceipt {
            request: p.key.request(),
            run_id: p.identities().run_id()?,
            conversation_id: p.identities().conversation_id,
        })
    }
}
fn validate_input_record(
    commit: &WorkItemExecutionAdmissionCommit,
) -> Result<(), DialogueStoreError> {
    use musteragent_persistence_codec::wire;
    use prost::Message;
    let batch = &commit.admission.batch;
    let p = &commit.preparation;
    let record = batch
        .append
        .records
        .iter()
        .find(|record| record.record_id == batch.message_record_id)
        .ok_or(DialogueStoreError::Corrupt)?;
    let event = wire::DomainEvent::decode(record.payload.as_slice())
        .map_err(|_| DialogueStoreError::Corrupt)?;
    if event.encode_to_vec() != record.payload {
        return Err(DialogueStoreError::Corrupt);
    }
    let Some(wire::domain_event::Event::MessageAccepted(message)) = event.event else {
        return Err(DialogueStoreError::Corrupt);
    };
    if message.message_id.as_ref().map(|id| id.value.as_str())
        != Some(p.identities().message_id.to_string().as_str())
        || message.role != wire::MessageRole::User as i32
        || message.content_ref
            != Some(musteragent_persistence_codec::encode_content_ref(
                p.input_content_ref(),
            ))
        || message.attachment_refs
            != p.input_attachments
                .iter()
                .map(musteragent_persistence_codec::dialogue_codec::attachment)
                .collect::<Vec<_>>()
        || message.presentation_template.is_some()
    {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(())
}
async fn insert_context(
    tx: &mut PgConnection,
    p: &WorkItemExecutionPreparation,
    now: UtcTimestamp,
) -> Result<(), DialogueStoreError> {
    let ids = p.identities();
    let s = p.key.scope;
    let tenant = p.key.actor.tenant_id;
    let report = p.report.is_some();
    let purpose = if report {
        "agent_work_item_report"
    } else {
        "agent_work_item_execution"
    };
    let service_ref = if report {
        "musteragent.work-item-report@1"
    } else {
        "musteragent.work-item@1"
    };
    let session_purpose = if report {
        "work_item_report"
    } else {
        "work_item_execution"
    };
    let subject = if report {
        "work_item_report"
    } else {
        "work_item"
    };
    sqlx::query("INSERT INTO conversation.conversations(conversation_id,tenant_id,workspace_id,kind,title,control_home_kind,control_device_id,home_epoch,lifecycle,created_by,created_at_ms,updated_at_ms,archived_at_ms,execution_owner_participant_id,purpose) VALUES($1,$2,NULL,4,NULL,1,NULL,1,1,$3,$4,$4,NULL,$5,$6)")
        .bind(ids.conversation_id.into_uuid()).bind(tenant.into_uuid()).bind(s.owner_id.into_uuid()).bind(now.unix_millis()).bind(ids.executor.into_uuid()).bind(purpose).execute(&mut *tx).await.map_err(db)?;
    // The service coordinates the accepted intent. The separate User member
    // audits actual user decisions; native purpose still denies public access.
    sqlx::query("INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,principal_ref,agent_ref,service_ref,display_name,avatar_content_id,joined_at_ms,left_at_ms) VALUES($1,$2,$3,3,NULL,NULL,$8,'Delegated task',NULL,$4,NULL),($5,$2,$3,2,NULL,$6,NULL,$7,NULL,$4,NULL)")
        .bind(ids.requested_by.into_uuid()).bind(tenant.into_uuid()).bind(ids.conversation_id.into_uuid()).bind(now.unix_millis()).bind(ids.executor.into_uuid()).bind(p.key.executing_agent_id.into_uuid()).bind(&p.configuration.profile.display_name).bind(service_ref).execute(&mut *tx).await.map_err(db)?;
    sqlx::query("INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,principal_ref,agent_ref,service_ref,display_name,avatar_content_id,joined_at_ms,left_at_ms) VALUES($1,$2,$3,1,$4,NULL,NULL,'User',NULL,$5,NULL)")
        .bind(ids.user.into_uuid()).bind(tenant.into_uuid()).bind(ids.conversation_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
    sqlx::query("INSERT INTO agent.dialogue_sessions(conversation_id,tenant_id,owner_id,agent_id,dialogue_id,source_intent_id,purpose,context_epoch,context_seq,created_at_ms,pinned_agent_version_id,permission_revision,manifest_revision,instructions_revision,conversation_purpose,subject_kind,source_dialogue_id,requesting_agent_id,work_item_id,work_item_goal_revision,work_item_control_revision,work_item_assignment_epoch,report_from_run_id) VALUES($1,$2,$3,$4,$5,$6,$20,$7,$8,$9,$10,$11,$12,$13,$21,$22,$14,$15,$16,$17,$18,$19,$23)")
      .bind(ids.conversation_id.into_uuid()).bind(tenant.into_uuid()).bind(s.owner_id.into_uuid()).bind(p.key.executing_agent_id.into_uuid()).bind(p.executing_agent.dialogue_id.into_uuid()).bind(p.work_item.source_intent_id.into_uuid())
      .bind(integer(p.context_cut.through.epoch)?).bind(integer(p.context_cut.through.seq.checked_add(1).ok_or(DialogueStoreError::Corrupt)?)?).bind(now.unix_millis())
      .bind(p.executing_agent.published_version_id.into_uuid()).bind(integer(p.configuration.permission_revision.get())?).bind(integer(p.configuration.manifest_revision.get())?).bind(integer(p.configuration.instructions_revision.get())?)
      .bind(p.work_item.source_dialogue_id.into_uuid()).bind(s.agent_id.into_uuid()).bind(p.key.work_item_id.into_uuid()).bind(integer(p.key.goal_revision.get())?).bind(integer(p.key.control_revision.get())?).bind(integer(p.key.assignment_epoch.get())?).bind(session_purpose).bind(purpose).bind(subject).bind(p.key.report_from_run_id.map(RunId::into_uuid)).execute(&mut *tx).await.map_err(db)?;
    Ok(())
}
