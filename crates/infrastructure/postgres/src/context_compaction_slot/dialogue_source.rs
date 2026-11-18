//! The original fenced compactor validates the logical source at its owner.
use super::*;
use musteragent_context::CompactionSource;

pub(super) async fn validate(
    tx: &mut sqlx::PgConnection,
    request: &ContextCompactionRequest,
) -> Result<(), PostgresContextCompactionSlotError> {
    let run = request.invocation.run_context.run_id;
    if !matches!(
        request.slot.source,
        CompactionSource::DialogueHistory { .. }
    ) {
        let tenant = request.invocation.run_context.tenant_id;
        let sealed: Option<String> = sqlx::query_scalar("SELECT ledger_cutoff::text FROM runtime.context_compaction_slots WHERE tenant_id=$1 AND run_id=$2 AND loop_iteration=$3 AND source_key='runtime-ledger' AND generation=1")
            .bind(tenant.into_uuid()).bind(run.into_uuid()).bind(i32::try_from(request.slot.loop_iteration)?).fetch_optional(&mut *tx).await?;
        let original_cut = sealed
            .as_deref()
            .map(str::parse::<u64>)
            .transpose()
            .map_err(|_| PostgresContextCompactionSlotError::InvalidContract)?;
        let latest = if original_cut.is_some() {
            let max = musteragent_identity::ConversationSeq::new(i64::MAX as u64)
                .map_err(|_| PostgresContextCompactionSlotError::InvalidContract)?;
            crate::control::consumed::latest(tx, tenant, run, max)
                .await
                .map_err(|error| match error {
                    crate::control::consumed::ConsumedControlError::Database(error) => {
                        PostgresContextCompactionSlotError::Database(error)
                    }
                    _ => PostgresContextCompactionSlotError::FenceConflict,
                })?
        } else {
            None
        };
        let superseding =
            latest.filter(|cut| original_cut.is_some_and(|original| cut.sequence.get() > original));
        if !request
            .slot
            .source
            .matches_superseding_control(superseding, request.slot.ledger_cutoff)
        {
            return Err(PostgresContextCompactionSlotError::FenceConflict);
        }
    }
    let full = crate::dialogue_context::run::run_cut(tx, run, true)
        .await
        .map_err(|_| PostgresContextCompactionSlotError::FenceConflict)?;
    match (&request.slot.source, full) {
        (CompactionSource::RuntimeLedger | CompactionSource::AfterControl(_), None) => Ok(()),
        (CompactionSource::RuntimeLedger | CompactionSource::AfterControl(_), Some(full)) => {
            crate::context_plan::dialogue::require_complete_cut(tx, run, &full)
                .await
                .map_err(|_| PostgresContextCompactionSlotError::InvalidContract)
        }
        (
            CompactionSource::DialogueHistory {
                coverage,
                parent_checkpoint_id,
            },
            Some(full),
        ) => {
            if !full.same_dialogue(coverage) || coverage.through.seq > full.through.seq {
                return Err(PostgresContextCompactionSlotError::InvalidContract);
            }
            let hash = crate::dialogue_context::chain_at(tx, &full, coverage.through.seq)
                .await
                .map_err(|_| PostgresContextCompactionSlotError::InvalidContract)?;
            if hash != coverage.source_chain_sha256 {
                return Err(PostgresContextCompactionSlotError::InvalidContract);
            }
            let records = request
                .summarized_record_ids
                .iter()
                .map(|id| id.into_uuid())
                .collect::<Vec<_>>();
            crate::context_plan::dialogue_checkpoint::verify_coverage(
                tx,
                run,
                coverage,
                *parent_checkpoint_id,
                &records,
            )
            .await
            .map_err(|_| PostgresContextCompactionSlotError::InvalidContract)
        }
        _ => Err(PostgresContextCompactionSlotError::InvalidContract),
    }
}
