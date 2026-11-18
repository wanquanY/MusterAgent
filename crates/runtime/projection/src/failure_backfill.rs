//! Restores additive Run failure facts when upgrading a compacted Snapshot.
//!
//! Logical cursor retention does not erase the canonical Domain Event spine.
//! Read only physically available facts, never reconstruct private payloads or
//! infer an error from a message, child Run, or mutable execution projection.

use musteragent_content_ref::Sha256;
use musteragent_event_envelope::{LedgerMetadata, LedgerRecordKind};
use musteragent_identity::{
    ContextBranchId, ConversationId, ConversationSeq, HomeEpoch, LedgerOffset, RunId, RunSeq,
    TenantId,
};
use musteragent_ledger::{CanonicalLedgerRecord, DomainEvent, LedgerPayload};
use musteragent_run::{RunEvent, RunStatus};
use musteragent_runtime_contract::ApplicationFuture;

use crate::{
    ConversationProjectionSnapshot, LedgerPayloadDecoder, ProjectedRun, ProjectedRunFailure,
    SubscriptionError,
};

/// Bounds each indexed reverse scan independently of Conversation size.
pub const PROJECTION_FAILURE_BACKFILL_PAGE_SIZE: usize = 64;

#[derive(Debug, Clone, Copy)]
pub struct ProjectionRunFailureQuery {
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub home_epoch: HomeEpoch,
    pub run_id: RunId,
    pub through_sequence: LedgerOffset,
    pub before_run_sequence: Option<RunSeq>,
}

#[derive(Debug, Clone)]
pub struct RetainedProjectionRunEvent {
    pub metadata: LedgerMetadata,
    pub context_branch_id: Option<ContextBranchId>,
    /// None means physical payload authority is unavailable. The metadata is
    /// still needed to avoid skipping an unavailable terminal fact.
    pub payload: Option<Vec<u8>>,
}

/// Projection-owned access to the retained Domain Event spine. Implementations
/// must use the Run index, descending Run sequence, and the fixed page bound.
/// This does not widen the public Ledger cursor or Content authorization APIs.
pub trait ProjectionRunFailureSource: Send + Sync {
    fn load_run_events(
        &self,
        query: ProjectionRunFailureQuery,
    ) -> ApplicationFuture<'_, Result<Vec<RetainedProjectionRunEvent>, SubscriptionError>>;
}

/// Enriches a verified legacy base before the normal reducer seals a new
/// generation. Current-generation cached Snapshots need no repeated scan.
pub async fn backfill_projection_run_failures<F>(
    snapshot: &mut ConversationProjectionSnapshot,
    source: &dyn ProjectionRunFailureSource,
    decoder: &dyn LedgerPayloadDecoder,
    includes: F,
) -> Result<(), SubscriptionError>
where
    F: Fn(Option<ContextBranchId>, ConversationSeq) -> bool + Send + Sync,
{
    for run in &mut snapshot.runs {
        if run.status != RunStatus::Failed || run.terminal_failure.is_some() {
            continue;
        }
        let query = ProjectionRunFailureQuery {
            tenant_id: snapshot.tenant_id,
            conversation_id: snapshot.conversation_id,
            home_epoch: snapshot.home_epoch,
            run_id: run.run_id,
            through_sequence: snapshot.through_conversation_seq,
            before_run_sequence: None,
        };
        run.terminal_failure = find_failure(run, query, source, decoder, &includes).await?;
    }
    Ok(())
}

async fn find_failure<F>(
    run: &ProjectedRun,
    mut query: ProjectionRunFailureQuery,
    source: &dyn ProjectionRunFailureSource,
    decoder: &dyn LedgerPayloadDecoder,
    includes: &F,
) -> Result<Option<ProjectedRunFailure>, SubscriptionError>
where
    F: Fn(Option<ContextBranchId>, ConversationSeq) -> bool + Send + Sync,
{
    let mut previous_conversation_sequence = None;
    loop {
        let page = source.load_run_events(query).await?;
        if page.len() > PROJECTION_FAILURE_BACKFILL_PAGE_SIZE {
            return Err(SubscriptionError::CorruptRecord);
        }
        let exhausted = page.len() < PROJECTION_FAILURE_BACKFILL_PAGE_SIZE;
        for record in page {
            let metadata = record.metadata;
            metadata
                .validate()
                .map_err(|_| SubscriptionError::CorruptRecord)?;
            let run_sequence = metadata.run_seq.ok_or(SubscriptionError::CorruptRecord)?;
            if metadata.tenant_id != query.tenant_id
                || metadata.conversation_id != query.conversation_id
                || metadata.home_epoch != query.home_epoch
                || metadata.run_id != Some(run.run_id)
                || metadata.activity_id != Some(run.activity_id)
                || metadata.record_kind != LedgerRecordKind::DomainEvent
                || metadata.conversation_seq.get() > query.through_sequence.get()
                || query
                    .before_run_sequence
                    .is_some_and(|before| run_sequence >= before)
                || previous_conversation_sequence
                    .is_some_and(|before| metadata.conversation_seq >= before)
            {
                return Err(SubscriptionError::CorruptRecord);
            }
            query.before_run_sequence = Some(run_sequence);
            previous_conversation_sequence = Some(metadata.conversation_seq);
            if !includes(record.context_branch_id, metadata.conversation_seq) {
                continue;
            }
            let Some(payload) = record.payload else {
                // An unavailable newer Domain Event may be the terminal fact.
                // Do not skip it and guess from an older failure.
                return Ok(None);
            };
            if Sha256::digest(&payload) != metadata.payload_hash {
                return Err(SubscriptionError::CorruptRecord);
            }
            let payload = decoder.decode(metadata.record_kind, &payload)?;
            let canonical = CanonicalLedgerRecord { metadata, payload };
            canonical
                .validate()
                .map_err(|_| SubscriptionError::CorruptRecord)?;
            match canonical.payload {
                LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Failed {
                    error, ..
                })) => {
                    error
                        .validate()
                        .map_err(|_| SubscriptionError::CorruptRecord)?;
                    return Ok(Some(ProjectedRunFailure {
                        error,
                        ledger_record_id: canonical.metadata.record_id,
                        correlation_id: canonical.metadata.correlation_id,
                        conversation_sequence: canonical.metadata.conversation_seq,
                    }));
                }
                LedgerPayload::DomainEvent(DomainEvent::Run(
                    RunEvent::Completed { .. }
                    | RunEvent::Cancelled { .. }
                    | RunEvent::Interrupted { .. },
                )) => return Err(SubscriptionError::CorruptRecord),
                _ => {}
            }
        }
        if exhausted {
            return Ok(None);
        }
    }
}
