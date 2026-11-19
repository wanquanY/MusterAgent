//! Restores additive Run <-> input Message admission links when upgrading a
//! compacted Snapshot.
//!
//! Only visible rows already retained by the Snapshot are enriched. Retracted
//! items are never reintroduced, and a retry Run links to its retried Message
//! without overwriting that Message's original `source_run_id`.

use std::collections::{BTreeMap, BTreeSet};

use musteragent_activity::ActivityKind;
use musteragent_conversation::MessageRole;
use musteragent_identity::{
    ConversationId, ConversationSeq, HomeEpoch, LedgerOffset, MessageId, RunId, TenantId,
};
use musteragent_runtime_contract::ApplicationFuture;

use crate::{ConversationProjectionSnapshot, SubscriptionError};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ProjectionRunInputQuery {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub through_sequence: LedgerOffset,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RetainedProjectionRunInput {
    pub run_id: RunId,
    pub input_message_id: MessageId,
    pub admitted_sequence: ConversationSeq,
}

/// Projection-owned access to immutable original Run admission message links.
/// Implementations must scope strictly to `(tenant_id, conversation_id, home_epoch)`
/// through `through_sequence` and exclude mid-run control messages and retries.
pub trait ProjectionRunInputSource: Send + Sync {
    fn load_run_inputs(
        &self,
        query: ProjectionRunInputQuery,
    ) -> ApplicationFuture<'_, Result<Vec<RetainedProjectionRunInput>, SubscriptionError>>;
}

/// Enriches a verified legacy Snapshot before the normal reducer seals a new
/// generation. Current-generation cached Snapshots need no repeated scan.
pub async fn backfill_projection_run_inputs(
    snapshot: &mut ConversationProjectionSnapshot,
    source: &dyn ProjectionRunInputSource,
) -> Result<(), SubscriptionError> {
    let mut runs_changed = false;
    for run in &mut snapshot.runs {
        if let Some(retry_message_id) = run.retry_source_message_id {
            if run.delegation_id.is_some() {
                return Err(SubscriptionError::CorruptRecord);
            }
            match run.input_message_id {
                None => {
                    run.input_message_id = Some(retry_message_id);
                    runs_changed = true;
                }
                Some(existing) if existing == retry_message_id => {}
                Some(_) => return Err(SubscriptionError::CorruptRecord),
            }
        }
    }

    let needs_run_lookup = snapshot
        .runs
        .iter()
        .any(|run| run.input_message_id.is_none() && run.delegation_id.is_none());
    let needs_message_lookup = snapshot
        .messages
        .iter()
        .any(|message| message.role == MessageRole::User && message.source_run_id.is_none());

    if !needs_run_lookup && !needs_message_lookup {
        if runs_changed {
            sync_active_runs(snapshot);
        }
        return Ok(());
    }

    let records = source
        .load_run_inputs(ProjectionRunInputQuery {
            tenant_id: snapshot.tenant_id,
            conversation_id: snapshot.conversation_id,
            home_epoch: snapshot.home_epoch,
            through_sequence: snapshot.through_conversation_seq,
        })
        .await?;

    let mut run_to_message = BTreeMap::<RunId, MessageId>::new();
    let mut message_to_run = BTreeMap::<MessageId, RunId>::new();
    for record in records {
        if record.admitted_sequence.get() > snapshot.through_conversation_seq.get() {
            return Err(SubscriptionError::CorruptRecord);
        }
        if let Some(existing) = run_to_message.insert(record.run_id, record.input_message_id)
            && existing != record.input_message_id
        {
            return Err(SubscriptionError::CorruptRecord);
        }
        if let Some(existing) = message_to_run.insert(record.input_message_id, record.run_id)
            && existing != record.run_id
        {
            return Err(SubscriptionError::CorruptRecord);
        }
    }

    for run in &mut snapshot.runs {
        if run.retry_source_message_id.is_some() {
            if run_to_message.contains_key(&run.run_id) {
                return Err(SubscriptionError::CorruptRecord);
            }
            continue;
        }
        if let Some(&message_id) = run_to_message.get(&run.run_id) {
            if run.delegation_id.is_some() {
                return Err(SubscriptionError::CorruptRecord);
            }
            match run.input_message_id {
                None => {
                    run.input_message_id = Some(message_id);
                    runs_changed = true;
                }
                Some(existing) if existing == message_id => {}
                Some(_) => return Err(SubscriptionError::CorruptRecord),
            }
        }
    }

    let chat_activities = snapshot
        .activities
        .iter()
        .filter(|activity| activity.kind == ActivityKind::Chat)
        .map(|activity| activity.activity_id)
        .collect::<BTreeSet<_>>();
    loop {
        let resolved_chat_inputs = snapshot
            .runs
            .iter()
            .filter(|run| chat_activities.contains(&run.activity_id) && run.delegation_id.is_none())
            .filter_map(|run| {
                run.input_message_id
                    .map(|message_id| (run.run_id, message_id))
            })
            .collect::<BTreeMap<_, _>>();
        let mut progressed = false;
        for run in &mut snapshot.runs {
            if run.input_message_id.is_none()
                && run.delegation_id.is_none()
                && chat_activities.contains(&run.activity_id)
                && let Some(parent_run_id) = run.parent_run_id
                && let Some(&message_id) = resolved_chat_inputs.get(&parent_run_id)
            {
                run.input_message_id = Some(message_id);
                runs_changed = true;
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }

    let visible_runs = snapshot
        .runs
        .iter()
        .map(|run| run.run_id)
        .collect::<BTreeSet<_>>();
    let mut visible_message_to_run = message_to_run
        .into_iter()
        .filter(|(_, run_id)| visible_runs.contains(run_id))
        .collect::<BTreeMap<_, _>>();
    for run in &snapshot.runs {
        if run.retry_source_message_id.is_none()
            && run.parent_run_id.is_none()
            && run.delegation_id.is_none()
            && let Some(message_id) = run.input_message_id
            && let Some(existing) = visible_message_to_run.insert(message_id, run.run_id)
            && existing != run.run_id
        {
            return Err(SubscriptionError::CorruptRecord);
        }
    }

    for message in &mut snapshot.messages {
        if message.role == MessageRole::User
            && let Some(&original_run_id) = visible_message_to_run.get(&message.message_id)
        {
            match message.source_run_id {
                None => message.source_run_id = Some(original_run_id),
                Some(existing) if existing == original_run_id => {}
                Some(_) => return Err(SubscriptionError::CorruptRecord),
            }
        }
    }

    if runs_changed {
        sync_active_runs(snapshot);
    }
    Ok(())
}

fn sync_active_runs(snapshot: &mut ConversationProjectionSnapshot) {
    snapshot.active_runs = snapshot
        .runs
        .iter()
        .filter(|item| !item.status.is_terminal())
        .cloned()
        .collect();
}
