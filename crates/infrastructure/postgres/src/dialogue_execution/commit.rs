use super::*;
use crate::admission::{self, PostgresAdmissionError};
use crate::admission_refs::RunAdmissionRef;
use crate::ledger::BeginAppend;
use musteragent_ledger::AdmissionReceipt;
use musteragent_runtime_snapshot::SNAPSHOT_SCHEMA_VERSION;

fn admission_error(error: PostgresAdmissionError) -> DialogueStoreError {
    use PostgresAdmissionError::*;
    match error {
        ConversationUnavailable => DialogueStoreError::Unavailable,
        ContentRefConflict | SnapshotGrantProjectionMismatch => DialogueStoreError::Corrupt,
        _ => DialogueStoreError::Infrastructure,
    }
}
impl PostgresDialogueExecutionStore {
    pub(super) async fn commit(
        &self,
        commit: DialogueExecutionAdmissionCommit,
    ) -> Result<AdmissionReceipt, DialogueStoreError> {
        commit.validate()?;
        validate_accepted_input_record(&commit)?;
        let p = &commit.preparation;
        let batch = &commit.admission.batch;
        let mut prepared = match self
            .admission
            .ledger
            .begin_append(&batch.append)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?
        {
            BeginAppend::Duplicate(receipt) => {
                self.admission
                    .validate_duplicate_content_admission(batch, &commit.admission.content_stage)
                    .await
                    .map_err(admission_error)?;
                let exists = self
                    .lookup(p.actor(), p.scope.source_intent_id)
                    .await?
                    .ok_or(DialogueStoreError::Corrupt)?;
                if exists.conversation_id != p.identities.conversation_id
                    || exists.run_id != batch.run.context.run_id
                {
                    return Err(DialogueStoreError::Corrupt);
                }
                return Ok(AdmissionReceipt {
                    message_sequence: receipt.first_sequence,
                    ledger: receipt,
                });
            }
            BeginAppend::Ready(value) => value,
        };
        let sequence = prepared.sequences()[0];
        let tx = prepared.transaction_mut();
        // Match original pre-admission cancellation order: account before intent.
        crate::dialogue_handoff::progress::lock_account(tx, p.actor()).await?;
        // Shared pre-Run linearization point. A denial never waits for Ledger,
        // so this lock order cannot create a queue -> Ledger cycle.
        super::lock_unresolved_intent(tx, p.actor(), p.scope.source_intent_id).await?;
        let current = load_preparation_for_contract(
            tx,
            p.actor(),
            p.scope.source_intent_id,
            true,
            self.delivery_contract,
        )
        .await?;
        if current != *p {
            return Err(DialogueStoreError::RevisionConflict);
        }
        let snapshot = self
            .admission
            .validate_new_admission_prefix(tx, batch)
            .await
            .map_err(admission_error)?;
        if !matches!(snapshot.schema_version.as_str(), SNAPSHOT_SCHEMA_VERSION)
            || snapshot.agent.agent_version_id != p.agent.published_version_id
            || snapshot.agent.agent_id != p.agent.agent_id
            || batch.runtime_snapshot.execution_grant.permission_mode
                != Some(musteragent_policy::PermissionMode::ServiceManaged)
        {
            return Err(DialogueStoreError::Corrupt);
        }
        musteragent_runtime_session::validate_dialogue_snapshot_source(&snapshot, p)
            .map_err(|_| DialogueStoreError::Corrupt)?;
        insert_context(tx, p, batch.run.admitted_at).await?;
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
        retain_snapshot_sources(
            tx,
            p.scope.tenant_id,
            batch.runtime_snapshot.runtime_snapshot_id,
            &snapshot,
        )
        .await?;
        let mut staged = prepared
            .stage(&batch.append)
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        {
            crate::dialogue_handoff::progress::refresh(
                staged.transaction_mut(),
                p.actor(),
                p.scope.source_intent_id,
                None,
                true,
            )
            .await?;
        }
        self.admission
            .audit
            .append_admission_allowed_in_transaction(
                staged.transaction_mut(),
                p.scope.tenant_id,
                batch.run.context.run_id,
            )
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        Ok(AdmissionReceipt {
            message_sequence: sequence,
            ledger: staged
                .commit()
                .await
                .map_err(|_| DialogueStoreError::Infrastructure)?,
        })
    }
}
async fn insert_context(
    tx: &mut PgConnection,
    p: &DialogueExecutionPreparation,
    now: UtcTimestamp,
) -> Result<(), DialogueStoreError> {
    let ids = p.identities;
    let s = p.scope;
    sqlx::query("INSERT INTO conversation.conversations(conversation_id,tenant_id,workspace_id,kind,title,control_home_kind,control_device_id,home_epoch,lifecycle,created_by,created_at_ms,updated_at_ms,archived_at_ms,execution_owner_participant_id,purpose) VALUES($1,$2,NULL,4,NULL,1,NULL,1,1,$3,$4,$4,NULL,$5,'dialogue_intent_coordination')")
        .bind(ids.conversation_id.into_uuid()).bind(s.tenant_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(now.unix_millis()).bind(ids.executor.into_uuid()).execute(&mut *tx).await.map_err(db)?;
    // The service coordinates the accepted intent. The separate User member
    // audits actual user decisions; native purpose still denies public access.
    sqlx::query("INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,principal_ref,agent_ref,service_ref,display_name,avatar_content_id,joined_at_ms,left_at_ms) VALUES($1,$2,$3,3,NULL,NULL,'musteragent.dialogue-intent@1','Dialogue intent',NULL,$4,NULL),($5,$2,$3,2,NULL,$6,NULL,$7,NULL,$4,NULL)")
        .bind(ids.requested_by.into_uuid()).bind(s.tenant_id.into_uuid()).bind(ids.conversation_id.into_uuid()).bind(now.unix_millis()).bind(ids.executor.into_uuid()).bind(s.agent_id.into_uuid()).bind(&p.configuration.profile.display_name).execute(&mut *tx).await.map_err(db)?;
    sqlx::query("INSERT INTO conversation.participants(participant_id,tenant_id,conversation_id,kind,principal_ref,agent_ref,service_ref,display_name,avatar_content_id,joined_at_ms,left_at_ms) VALUES($1,$2,$3,1,$4,NULL,NULL,'User',NULL,$5,NULL)")
        .bind(ids.user.into_uuid()).bind(s.tenant_id.into_uuid()).bind(ids.conversation_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(now.unix_millis()).execute(&mut *tx).await.map_err(db)?;
    sqlx::query("INSERT INTO agent.dialogue_sessions(conversation_id,tenant_id,owner_id,agent_id,dialogue_id,source_intent_id,purpose,context_epoch,context_seq,created_at_ms,pinned_agent_version_id,permission_revision,manifest_revision,instructions_revision,conversation_purpose) VALUES($1,$2,$3,$4,$5,$6,'intent_coordination',$7,$8,$9,$10,$11,$12,$13,'dialogue_intent_coordination')")
        .bind(ids.conversation_id.into_uuid()).bind(s.tenant_id.into_uuid()).bind(s.owner_id.into_uuid()).bind(s.agent_id.into_uuid()).bind(s.dialogue_id.into_uuid()).bind(s.source_intent_id.into_uuid()).bind(integer(s.context_watermark.epoch)?).bind(integer(s.context_watermark.seq)?).bind(now.unix_millis()).bind(p.agent.published_version_id.into_uuid()).bind(integer(p.configuration.permission_revision.get())?).bind(integer(p.configuration.manifest_revision.get())?).bind(integer(p.configuration.instructions_revision.get())?).execute(tx).await.map_err(db)?;
    Ok(())
}

/// Lock exact ready source rows before publishing the retention root. Global
/// Runtime release sources are owned outside tenant GC; all other sources must
/// belong to this tenant. A ContentId alone never proves access.
pub(crate) async fn retain_snapshot_sources(
    tx: &mut PgConnection,
    tenant: TenantId,
    snapshot_id: RuntimeSnapshotId,
    snapshot: &musteragent_runtime_snapshot::RuntimeSnapshotContent,
) -> Result<(), DialogueStoreError> {
    let mut references = snapshot
        .prompt
        .sections
        .iter()
        .map(|section| &section.content_ref)
        .collect::<Vec<_>>();
    references.sort_by_key(|reference| reference.content_id.into_uuid());
    references.dedup();
    for reference in references {
        let row=sqlx::query("SELECT tenant_id,ownership_kind FROM artifact.content_refs WHERE content_id=$1 AND media_type=$2 AND byte_length=$3::bigint AND content_hash=$4 AND state=2 AND ((tenant_id=$5 AND ownership_kind=1) OR (tenant_id IS NULL AND ownership_kind=3)) FOR SHARE")
            .bind(reference.content_id.into_uuid()).bind(reference.media_type.as_str()).bind(reference.byte_length.to_string()).bind(reference.sha256.into_bytes().to_vec()).bind(tenant.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Corrupt)?;
        if row
            .try_get::<Option<uuid::Uuid>, _>("tenant_id")
            .map_err(db)?
            .is_some()
        {
            sqlx::query("INSERT INTO capability.runtime_snapshot_content_references(runtime_snapshot_id,tenant_id,content_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
                .bind(snapshot_id.into_uuid()).bind(tenant.into_uuid()).bind(reference.content_id.into_uuid()).execute(&mut *tx).await.map_err(db)?;
        }
    }
    Ok(())
}

/// AdmissionMessage intentionally carries only the body projection. The exact
/// accepted attachment array lives in the original MessageAccepted Ledger fact.
fn validate_accepted_input_record(
    commit: &DialogueExecutionAdmissionCommit,
) -> Result<(), DialogueStoreError> {
    use musteragent_persistence_codec::wire;
    use prost::Message;
    let batch = &commit.admission.batch;
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
    let p = &commit.preparation;
    let expected = p
        .intent
        .input_attachments
        .iter()
        .map(|input| musteragent_persistence_codec::dialogue_codec::attachment(&input.attachment))
        .collect::<Vec<_>>();
    if message.message_id.as_ref().map(|id| id.value.as_str())
        != Some(p.identities.message_id.to_string().as_str())
        || message.role != wire::MessageRole::User as i32
        || message.content_ref
            != Some(musteragent_persistence_codec::encode_content_ref(
                &p.intent.content_ref,
            ))
        || message.attachment_refs != expected
        || message.presentation_template.is_some()
    {
        return Err(DialogueStoreError::Corrupt);
    }
    Ok(())
}
