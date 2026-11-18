//! Revalidate the original source under the same locks that commit its Run.
use super::*;
use crate::admission::PostgresAdmissionError;
use musteragent_ledger::{
    StandingGoalAdmissionBatch, StandingGoalAdmissionContext, StandingGoalAdmissionSource,
};
use musteragent_runtime_snapshot::RuntimeSnapshotContent;

pub(crate) async fn validate(
    tx: &mut PgConnection,
    batch: &StandingGoalAdmissionBatch,
    snapshot: &RuntimeSnapshotContent,
) -> Result<bool, PostgresAdmissionError> {
    let StandingGoalAdmissionSource::OwnerGoal {
        previous_run_id,
        binding_revision,
        source_sha256,
    } = batch.source
    else {
        return Err(PostgresAdmissionError::StandingGoalInvariant);
    };
    let goal = load_goal_tx(tx, batch.actor.tenant_id, batch.goal_id)
        .await
        .map_err(super::super::admission::storage_error)?
        .ok_or(PostgresAdmissionError::StandingGoalInvariant)?;
    let Some(binding) = load_binding(tx, &goal)
        .await
        .map_err(super::super::admission::storage_error)?
    else {
        return Ok(false);
    };
    if binding.revision != binding_revision || binding.scope.actor() != batch.actor {
        return Ok(false);
    }
    let current = match preparation::load(
        tx,
        OwnerGoalContinuationRequest {
            scope: binding.scope,
            pursuit_id: batch.pursuit_id,
            fence: batch.fence.clone(),
            phase: match &batch.context {
                StandingGoalAdmissionContext::Continuation { .. } => {
                    OwnerGoalAdmissionPhase::Execution
                }
                StandingGoalAdmissionContext::Judge { judge_input_ref } => {
                    OwnerGoalAdmissionPhase::Evaluation {
                        source_run_id: previous_run_id
                            .ok_or(PostgresAdmissionError::StandingGoalInvariant)?,
                        judge_input_ref: judge_input_ref.clone(),
                    }
                }
            },
        },
        true,
    )
    .await
    {
        Ok(value) => value,
        Err(
            DialogueStoreError::RevisionConflict
            | DialogueStoreError::Unavailable
            | DialogueStoreError::Inactive,
        ) => return Ok(false),
        Err(_) => return Err(PostgresAdmissionError::StandingGoalInvariant),
    };
    let facts = musteragent_persistence_codec::owner_goal_admission_codec::facts(&current)
        .map_err(|_| PostgresAdmissionError::StandingGoalInvariant)?;
    // The original admission transaction owns both the first Run and the
    // standing ceiling. A stale caller cannot widen or replace a retained one.
    if batch
        .continuation_authority
        .as_ref()
        .is_none_or(|value| value.is_empty() || value.len() > 512 * 1024)
        || current
            .standing_authority
            .as_ref()
            .is_some_and(|value| batch.continuation_authority.as_ref() != Some(value))
    {
        return Err(PostgresAdmissionError::StandingGoalInvariant);
    }
    let identity = current
        .identities()
        .map_err(|_| PostgresAdmissionError::StandingGoalInvariant)?;
    let context_matches = match (&batch.context, &current.phase) {
        (
            StandingGoalAdmissionContext::Continuation {
                objective_ref,
                previous_evaluation_summary_ref,
                input_refs,
            },
            OwnerGoalAdmissionPhase::Execution,
        ) => {
            current.input_refs == *input_refs
                && current.processing_rules.objective_ref == *objective_ref
                && current.previous_evaluation_summary_ref == *previous_evaluation_summary_ref
        }
        (
            StandingGoalAdmissionContext::Judge { judge_input_ref },
            OwnerGoalAdmissionPhase::Evaluation {
                judge_input_ref: expected,
                ..
            },
        ) => judge_input_ref == expected,
        _ => false,
    };
    if facts.facts_sha256.as_slice() != source_sha256.into_bytes().as_slice()
        || current.previous_run_id != previous_run_id
        || identity
            .run_id()
            .map_err(|_| PostgresAdmissionError::StandingGoalInvariant)?
            != batch.run.context.run_id
        || identity.conversation_id != batch.append.conversation_id
        || identity.executor != batch.run.context.executor_participant_id
        || identity.requested_by != batch.run.context.requested_by_participant_id
        || current.private_context_home_epoch != batch.append.expected_home_epoch
        || current.private_context_offset != batch.append.expected_offset
        || !context_matches
        || batch.runtime_snapshot.execution_grant.nonce.as_str()
            != format!("owner_goal_source_{source_sha256}")
        || batch.runtime_snapshot.execution_grant.permission_mode
            != Some(musteragent_policy::PermissionMode::ServiceManaged)
        || musteragent_runtime_session::validate_owner_goal_snapshot_source(snapshot, &current)
            .is_err()
    {
        return Ok(false);
    }
    Ok(true)
}

/// This is a read projection only. Tool/context authorization always reads the
/// original sealed Snapshot and current source, never these directory columns.
pub(crate) async fn project_run(
    tx: &mut PgConnection,
    batch: &StandingGoalAdmissionBatch,
) -> Result<(), PostgresAdmissionError> {
    use musteragent_runtime_snapshot::SnapshotCodec;
    let snapshot = musteragent_persistence_codec::ProtobufSnapshotCodec
        .decode_content(&batch.runtime_snapshot.encoded_snapshot)
        .map_err(|_| PostgresAdmissionError::StandingGoalInvariant)?;
    let authority = snapshot
        .owner_goal_authority
        .ok_or(PostgresAdmissionError::StandingGoalInvariant)?;
    if matches!(batch.context, StandingGoalAdmissionContext::Judge { .. }) {
        let parent = batch
            .source
            .parent_run_id()
            .ok_or(PostgresAdmissionError::StandingGoalInvariant)?;
        let rows = sqlx::query(
            "INSERT INTO runtime.goal_run_processing_rules(tenant_id,goal_id,run_id,rules_version)
            SELECT tenant_id,goal_id,$4,rules_version FROM runtime.goal_run_processing_rules
            WHERE tenant_id=$1 AND goal_id=$2 AND run_id=$3",
        )
        .bind(batch.actor.tenant_id.into_uuid())
        .bind(batch.goal_id.into_uuid())
        .bind(parent.into_uuid())
        .bind(batch.run.context.run_id.into_uuid())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if rows != 1 {
            return Err(PostgresAdmissionError::StandingGoalInvariant);
        }
    }
    let rows = sqlx::query("UPDATE runtime.goal_run_processing_rules SET owner_pursuit_id=$4,dialogue_context_epoch=$5,dialogue_context_seq=$6
        WHERE tenant_id=$1 AND goal_id=$2 AND run_id=$3 AND owner_pursuit_id IS NULL")
        .bind(batch.actor.tenant_id.into_uuid()).bind(batch.goal_id.into_uuid()).bind(batch.run.context.run_id.into_uuid())
        .bind(authority.pursuit_id.into_uuid()).bind(authority.context_cut.through.epoch as i64)
        .bind(authority.context_cut.through.seq as i64).execute(tx).await?.rows_affected();
    if rows != 1 {
        return Err(PostgresAdmissionError::StandingGoalInvariant);
    }
    Ok(())
}
