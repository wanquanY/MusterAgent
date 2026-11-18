//! Fenced delivery commits share the existing Runtime lane, then acquire
//! dialogue authority locks. No dialogue lock is held while waiting for a Run.
pub(crate) mod candidate_resolution;
pub(crate) mod completion;
pub(crate) mod context_refs;
mod device_tool;
mod native_interaction;
pub(crate) mod task_result;
pub use native_interaction::PostgresNativeDialogueInteractionStore;
pub(crate) mod interaction;
mod native_terminal;
mod native_tool;
pub use native_terminal::PostgresDeviceTerminalStore;
mod work_item_device;
pub use device_tool::PostgresDeviceToolPublication;
pub(crate) use work_item_device::{
    load_work_item_device_entries, verify_work_item_device_cut,
    verify_work_item_device_terminal_cut,
};
pub(crate) mod file_reference;
mod references;
pub(crate) mod reply;
pub(crate) mod terminal;
pub(crate) mod work_item_erasure;
pub(crate) mod work_item_input;
pub use native_tool::PostgresFencedDialogueToolPublication;

pub(crate) mod artifact;

use crate::{dialogue_codec::*, execution::lock_and_validate_fence};
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_persistence_codec::{dialogue_codec as codec, wire};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use musteragent_telemetry::{DbCommitKind, DbTransactionPhase};
use sqlx::{Row, postgres::PgRow};
use std::num::NonZeroU64;
type Error = DialoguePublicationError;

fn transaction_error(phase: DbTransactionPhase, error: crate::PostgresTransactionError) -> Error {
    tracing::warn!(
        transaction_phase = phase.as_str(),
        failure_class = ?error.failure_class(),
        commit_unknown = matches!(error, crate::PostgresTransactionError::CommitOutcomeUnknown { .. }),
        "Dialogue publication transaction failed; original durable delivery must settle the outcome"
    );
    // The original delivery/obligation is the reconciliation authority. An
    // unknown acknowledgement is never success and never requests a new model.
    DialogueStoreError::Infrastructure.into()
}

#[derive(Clone)]
pub struct PostgresDialoguePublicationStore {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresDialoguePublicationStore {
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        Self { database }
    }

    /// Resolve the model-visible handle through a successful original Tool
    /// receipt. Publication revalidates the resolved immutable version under
    /// the original Run fence. Text is persisted by the Content owner separately.
    pub(crate) async fn resolve_candidate_object(
        &self,
        run_id: RunId,
        draft: &PublicDeliveryDraft,
    ) -> Result<Option<PublicDeliveryContent>, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Acquire, error))?;
        let source = load_source(&mut tx, run_id).await?;
        candidate_resolution::resolve(&mut tx, &source, run_id, draft).await
    }

    /// V7 validates all objects against one Publication source. Source failure
    /// remains an outer error; it must never masquerade as a missing handle.
    pub(crate) async fn resolve_candidate_objects(
        &self,
        run_id: RunId,
        drafts: &[ParsedPublicDelivery],
    ) -> Result<Vec<candidate_resolution::CandidateObject>, Error> {
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Acquire, error))?;
        let source = load_source(&mut tx, run_id).await?;
        let mut objects = Vec::with_capacity(drafts.len());
        for draft in drafts {
            objects.push(
                match candidate_resolution::resolve(&mut tx, &source, run_id, &draft.content).await
                {
                    Ok(object) => candidate_resolution::CandidateObject::Available(object),
                    Err(Error::Store(DialogueStoreError::Unavailable)) => {
                        candidate_resolution::CandidateObject::ReferenceUnavailable
                    }
                    Err(error) => return Err(error),
                },
            );
        }
        Ok(objects)
    }

    async fn append_inner(
        &self,
        command: PublicEntryAppend,
    ) -> Result<DialoguePublicationReceipt, Error> {
        command.validate()?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Acquire, error))?;
        let source = load_source(&mut tx, command.fence.run_id).await?;
        if source.permission_revision != command.expected_permission_revision.get() {
            return Err(DialogueStoreError::RevisionConflict.into());
        }
        // Same physical commit lane used by canonical Ledger writers. A read
        // cannot create a stream or upgrade an ordinary conversation's owner.
        let epoch: Option<String> = sqlx::query_scalar(
            "SELECT home_epoch::text FROM event_store.ledger_streams
             WHERE tenant_id=$1 AND conversation_id=$2 FOR UPDATE",
        )
        .bind(source.actor.tenant_id.into_uuid())
        .bind(source.conversation_id.into_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        if epoch.as_deref() != Some(command.fence.home_epoch.get().to_string().as_str()) {
            return Err(Error::StaleFence);
        }
        if let Some(receipt) = replay(&mut tx, &source, &command).await? {
            tx.commit()
                .await
                .map_err(|error| transaction_error(DbTransactionPhase::Commit, error))?;
            return Ok(receipt);
        }
        validate_fence(&mut tx, &command).await?;
        // The original fence allows cancellation cleanup. New public delivery
        // cannot use that exception to publish a stale success after cancel.
        let cancelled: bool = sqlx::query_scalar(
            "SELECT status<>2 OR cancel_requested_at_ms IS NOT NULL FROM runtime.runs WHERE run_id=$1",
        ).bind(command.fence.run_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        if cancelled {
            return Err(Error::Cancelled);
        }

        let entry = publish_in_transaction(
            &mut tx,
            &source,
            &Delivery {
                delivery_id: command.delivery_id,
                run_id: command.fence.run_id,
                purpose: command.purpose,
                content: command.content.clone(),
                fingerprint: command.fingerprint(),
                source: PublicationSource::Model,
            },
        )
        .await?;
        // Account/content locks may wait; the original lease must still hold.
        validate_fence(&mut tx, &command).await?;
        tx.commit()
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Commit, error))?;
        Ok(DialoguePublicationReceipt {
            entry,
            replayed: false,
        })
    }
}
impl DialoguePublicationStore for PostgresDialoguePublicationStore {
    fn append(
        &self,
        command: PublicEntryAppend,
    ) -> ApplicationFuture<'_, Result<DialoguePublicationReceipt, Error>> {
        Box::pin(self.append_inner(command))
    }
}

mod source;
use source::{PublicationHome, PublicationSubject, Source, load_source};
async fn validate_fence(
    tx: &mut sqlx::PgConnection,
    command: &PublicEntryAppend,
) -> Result<(), Error> {
    let now = crate::ledger::database_now_ms(&mut *tx)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    lock_and_validate_fence(
        &mut *tx,
        &command.fence,
        command.expected_last_run_sequence,
        now,
    )
    .await
    .map_err(|error| match error {
        crate::execution::PostgresExecutionError::StaleFence => Error::StaleFence,
        _ => Error::Store(DialogueStoreError::Infrastructure),
    })
}
async fn replay(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    command: &PublicEntryAppend,
) -> Result<Option<DialoguePublicationReceipt>, Error> {
    let row = sqlx::query(
        "SELECT e.*,p.payload_sha256,p.source_run_id FROM agent.dialogue_publications p
        JOIN agent.agent_dialogue_entries e USING(entry_id)
        WHERE p.tenant_id=$1 AND p.owner_id=$2 AND p.agent_id=$3 AND p.delivery_id=$4",
    )
    .bind(source.actor.tenant_id.into_uuid())
    .bind(source.actor.principal_id.into_uuid())
    .bind(source.agent_id.into_uuid())
    .bind(command.delivery_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    row.map(|row| {
        if row.try_get::<Vec<u8>, _>("payload_sha256").map_err(db)?
            != command.fingerprint().into_bytes()
            || row.try_get::<uuid::Uuid, _>("source_run_id").map_err(db)?
                != command.fence.run_id.into_uuid()
        {
            return Err(DialogueStoreError::IdempotencyConflict.into());
        }
        Ok(DialoguePublicationReceipt {
            entry: crate::agent_dialogue::entry_row(&row)?,
            replayed: true,
        })
    })
    .transpose()
}
async fn insert_entry(
    tx: &mut sqlx::PgConnection,
    entry: &AgentDialogueEntry,
) -> Result<(), Error> {
    let (kind, content, interaction, artifact, version) = match &entry.content {
        AgentDialogueContent::AgentMessage { content_ref } => (
            "agent_message",
            Some(content_bytes(content_ref)),
            None,
            None,
            None,
        ),
        AgentDialogueContent::Interaction { interaction_id } => (
            "interaction",
            None,
            Some(interaction_id.into_uuid()),
            None,
            None,
        ),
        AgentDialogueContent::Artifact {
            artifact_id,
            artifact_version_id,
        } => (
            "artifact",
            None,
            None,
            Some(artifact_id.into_uuid()),
            Some(artifact_version_id.into_uuid()),
        ),
        AgentDialogueContent::PartnerCreated(_) => {
            return Err(DialogueStoreError::Unavailable.into());
        }
        AgentDialogueContent::UserMessage { .. } | AgentDialogueContent::Erased { .. } => {
            return Err(DialogueStoreError::Corrupt.into());
        }
    };
    sqlx::query(
        "INSERT INTO agent.agent_dialogue_entries
        (entry_id,tenant_id,owner_id,dialogue_id,epoch,seq,kind,source_intent_id,author_agent_id,
         content_ref,interaction_id,artifact_id,artifact_version_id,visible_revision,created_at_ms,
         presentation)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,1,$14,$15)",
    )
    .bind(entry.entry_id.into_uuid())
    .bind(entry.tenant_id.into_uuid())
    .bind(entry.owner_id.into_uuid())
    .bind(entry.dialogue_id.into_uuid())
    .bind(integer(entry.cursor.epoch)?)
    .bind(integer(entry.cursor.seq)?)
    .bind(kind)
    .bind(entry.source_intent_id.map(UserIntentId::into_uuid))
    .bind(entry.author_agent_id.map(AgentId::into_uuid))
    .bind(content)
    .bind(interaction)
    .bind(artifact)
    .bind(version)
    .bind(entry.created_at.unix_millis())
    .bind(entry.presentation.as_str())
    .execute(&mut *tx)
    .await
    .map_err(db)?;
    Ok(())
}

struct Delivery {
    delivery_id: CommandId,
    run_id: RunId,
    purpose: DialoguePublicationPurpose,
    content: PublicDeliveryContent,
    fingerprint: musteragent_content_ref::Sha256,
    source: PublicationSource,
}
#[derive(Clone)]
enum PublicationSource {
    Model,
    TaskResultOwner(task_result::AuthorizedResultItem),
    InteractionOwner(InteractionId),
    TerminalOwner(LedgerRecordId),
    NativeTerminalOwner(RunId),
    ToolOwner(InvocationId),
    ReturnedFileToolOwner(InvocationId, task_result::AuthorizedResultItem),
    FileToolOwner(InvocationId, file_reference::AuthorizedFileDelivery),
}

/// Shared lock order: Runtime/custody -> account -> dialogue/config -> Resource.
/// File resolution runs only after these locks, which remain held through commit.
async fn lock_publication_authority(
    tx: &mut sqlx::PgConnection,
    source: &Source,
) -> Result<AgentDialogue, Error> {
    let space: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT s.space_id FROM agent.owner_spaces s
             JOIN agent.owned_agents a USING(space_id)
             WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 FOR UPDATE OF s",
    )
    .bind(source.agent_id.into_uuid())
    .bind(source.actor.tenant_id.into_uuid())
    .bind(source.actor.principal_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    if space.is_none() {
        return Err(DialogueStoreError::Unavailable.into());
    }
    let row = sqlx::query(
        "SELECT d.*,a.status,c.permission_revision,c.permission_mode FROM agent.agent_dialogues d
             JOIN agent.owned_agents a USING(agent_id)
             JOIN agent.owned_agent_configurations c USING(agent_id)
             WHERE d.dialogue_id=$1 AND d.tenant_id=$2 AND d.owner_id=$3 AND d.agent_id=$4
             FOR UPDATE OF d FOR SHARE OF a,c",
    )
    .bind(source.dialogue_id.into_uuid())
    .bind(source.actor.tenant_id.into_uuid())
    .bind(source.actor.principal_id.into_uuid())
    .bind(source.agent_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(DialogueStoreError::Unavailable)?;
    if row.try_get::<&str, _>("status").map_err(db)? != "active" {
        return Err(DialogueStoreError::Inactive.into());
    }
    match source.execution_home {
        PublicationHome::Cloud => {
            if unsigned(row.try_get("permission_revision").map_err(db)?)?
                != source.permission_revision
            {
                return Err(DialogueStoreError::RevisionConflict.into());
            }
        }
        PublicationHome::Device(frozen_mode) => {
            // Keep the original Native ceiling under the final configuration
            // lock, just as context and resource custody do. A compatible mode
            // change never rewrites the frozen source or elevates its authority.
            crate::agent_resource::device_custody::require_frozen_permission(
                &row,
                source.permission_revision,
                frozen_mode,
            )?;
        }
    }
    if matches!(source.subject, PublicationSubject::WorkItem(_))
        && source.execution_home == PublicationHome::Cloud
    {
        let current: bool =
            sqlx::query_scalar("SELECT collaboration.work_item_session_is_current($1)")
                .bind(source.conversation_id.into_uuid())
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if !current {
            return Err(DialogueStoreError::RevisionConflict.into());
        }
    }
    if let PublicationSubject::Goal { run_id, .. } = source.subject {
        let cut = crate::dialogue_context::run::run_cut(tx, run_id, true)
            .await?
            .ok_or(DialogueStoreError::Unavailable)?;
        if cut.tenant_id != source.actor.tenant_id
            || cut.owner_id != source.actor.principal_id
            || cut.agent_id != source.agent_id
            || cut.dialogue_id != source.dialogue_id
        {
            return Err(DialogueStoreError::Unavailable.into());
        }
    }
    dialogue_row(&row).map_err(Into::into)
}

async fn publish_in_transaction(
    tx: &mut sqlx::PgConnection,
    source: &Source,
    delivery: &Delivery,
) -> Result<AgentDialogueEntry, Error> {
    task_result::require_delivery_contract(tx, source, delivery).await?;
    let dialogue = lock_publication_authority(tx, source).await?;
    let cursor = dialogue
        .head
        .next()
        .map_err(|_| DialogueStoreError::RevisionConflict)?;
    let entry_id = match &delivery.source {
        PublicationSource::TaskResultOwner(item)
        | PublicationSource::ReturnedFileToolOwner(_, item) => item.entry_id,
        _ => DialogueEntryId::generate(),
    };
    references::validate_and_pin(
        &mut *tx,
        source,
        delivery.run_id,
        entry_id,
        &delivery.content,
        &delivery.source,
    )
    .await?;
    let now = crate::ledger::database_now_ms(&mut *tx)
        .await
        .map_err(|_| DialogueStoreError::Infrastructure)?;
    let entry = AgentDialogueEntry {
        entry_id,
        dialogue_id: source.dialogue_id,
        tenant_id: source.actor.tenant_id,
        owner_id: source.actor.principal_id,
        cursor,
        source_intent_id: source.subject.public_intent(),
        author_agent_id: Some(source.agent_id),
        visible_revision: NonZeroU64::MIN,
        content: delivery.content.public_content(),
        created_at: now,
        presentation: AgentDialoguePresentation::for_purpose(delivery.purpose),
    };
    insert_entry(&mut *tx, &entry).await?;
    // Progress narration belongs to the live chat, not to external channels.
    let progress = delivery.purpose == DialoguePublicationPurpose::Progress;
    // The original Cloud statement and published bytes remain unchanged.
    // Only the explicit Device source uses the forward schema's typed branch.
    let (home_column, home_value) = source.execution_home.insert_clause();
    sqlx::query(&format!("INSERT INTO agent.dialogue_publications
            (tenant_id,owner_id,agent_id,dialogue_id,delivery_id,entry_id,source_intent_id,
             source_conversation_id,source_run_id,purpose,permission_revision,payload_sha256,created_at_ms,source_kind,source_interaction_id,source_terminal_record_id,source_tool_invocation_id,device_subject_kind,source_native_terminal_run_id,source_goal_id,source_task_result_item_id{home_column})
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21{home_value})"))
            .bind(entry.tenant_id.into_uuid()).bind(entry.owner_id.into_uuid()).bind(source.agent_id.into_uuid())
            .bind(entry.dialogue_id.into_uuid()).bind(delivery.delivery_id.into_uuid()).bind(entry_id.into_uuid())
            .bind(source.subject.intent().map(UserIntentId::into_uuid)).bind(source.conversation_id.into_uuid()).bind(delivery.run_id.into_uuid())
            .bind(delivery.purpose.as_str()).bind(integer(source.permission_revision)?)
            .bind(delivery.fingerprint.into_bytes().to_vec()).bind(now.unix_millis())
            .bind(match &delivery.source { PublicationSource::Model => "model_delivery", PublicationSource::TaskResultOwner(_)=>"task_result_owner", PublicationSource::InteractionOwner(_) => "interaction_owner", PublicationSource::TerminalOwner(_) => "terminal_owner", PublicationSource::NativeTerminalOwner(_) => "native_terminal_owner", PublicationSource::ReturnedFileToolOwner(..) | PublicationSource::ToolOwner(_) | PublicationSource::FileToolOwner(..) => "tool_owner" })
            .bind(match &delivery.source { PublicationSource::InteractionOwner(id) => Some(id.into_uuid()), _ => None })
            .bind(match &delivery.source { PublicationSource::TerminalOwner(id) => Some(id.into_uuid()), _ => None })
            .bind(match &delivery.source { PublicationSource::ReturnedFileToolOwner(id,_) | PublicationSource::ToolOwner(id) | PublicationSource::FileToolOwner(id, _) => Some(id.into_uuid()), _ => None })
            .bind(if matches!(source.execution_home, PublicationHome::Device(_)) && matches!(source.subject, PublicationSubject::WorkItem(_)) { "work_item" } else { "intent" })
            .bind(match &delivery.source { PublicationSource::NativeTerminalOwner(id) => Some(id.into_uuid()), _ => None })
            .bind(source.subject.goal().map(GoalId::into_uuid))
            .bind(match &delivery.source {PublicationSource::TaskResultOwner(item)=>Some(item.item_id),_=>None})
            .execute(&mut *tx).await.map_err(db)?;
    if !progress && matches!(entry.content, AgentDialogueContent::AgentMessage { .. }) {
        crate::agent_work_creations::publish(
            tx,
            source.actor,
            source.agent_id,
            delivery.run_id,
            entry_id,
        )
        .await?;
    }
    sqlx::query("UPDATE agent.agent_dialogues SET head_seq=$2 WHERE dialogue_id=$1")
        .bind(entry.dialogue_id.into_uuid())
        .bind(integer(cursor.seq)?)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
    outbox(
        &mut *tx,
        source.actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "agent_dialogue",
            aggregate: source.dialogue_id.into_uuid(),
            cursor,
            event: "public_entry_published",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::PublicEntryPublished(
                    codec::entry(&entry),
                )),
            },
            at: now,
        },
    )
    .await?;
    Ok(entry)
}
