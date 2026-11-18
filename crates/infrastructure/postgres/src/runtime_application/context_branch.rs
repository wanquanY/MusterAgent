use std::collections::BTreeMap;

use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_conversation::{ContextBranchOrigin, ContextBranchView};
use musteragent_identity::{
    ActivityId, Actor, ContentId, ContextBranchId, ConversationId, ConversationSeq, CorrelationId,
    HomeEpoch, LedgerOffset, LedgerRecordId, MessageId, PrincipalId, RunId, UtcTimestamp,
};
use musteragent_ledger::{AdmissionReceipt, AppendDisposition, AppendReceipt};
use musteragent_runtime_contract::{ErrorCode, RuntimeError};
use musteragent_runtime_session::{
    ActivateContextBranchCommit, ContextBranchAuthority, EditAndResubmitAuthority,
    EditAndResubmitCommit, ForkContextAuthority, ForkContextCommit, runtime_error,
};
use sqlx::{Postgres, Row as _};

use super::context_reachability::ReachableContextPoint;
use super::{
    BeginAppend, PostgresRuntimeApplicationStore, corrupt, dependency, id, map_admission,
    map_ledger, parse_u64,
};
use crate::admission::{
    authorize_content, consume_execution_grant, insert_activity, insert_execution_grant_routes,
    insert_memory_selections, insert_message, insert_run, insert_runtime_snapshot,
    validate_admission_authority,
};

const MAX_CONTEXT_BRANCHES_PER_CONVERSATION: i64 = 256;

pub(super) struct ConversationRuntimeState {
    pub(super) active_activity_ids: Vec<ActivityId>,
    pub(super) active_run_ids: Vec<RunId>,
    pub(super) context_branches: Vec<ContextBranchView>,
}

pub(super) async fn load_conversation_runtime_state(
    pool: &sqlx::PgPool,
    actor: Actor,
    conversation_id: ConversationId,
    through_offset: LedgerOffset,
    correlation_id: CorrelationId,
) -> Result<ConversationRuntimeState, RuntimeError> {
    let active_activity_ids = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT activity_id FROM activity.activities
         WHERE tenant_id = $1 AND conversation_id = $2 AND status IN (1, 2, 3)
         ORDER BY created_at_ms, activity_id",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_all(pool)
    .await
    .map_err(|_| dependency(correlation_id))?
    .into_iter()
    .map(|value| ActivityId::from_uuid(value).map_err(|_| corrupt(correlation_id)))
    .collect::<Result<Vec<_>, _>>()?;
    let active_run_ids = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT run_id FROM runtime.runs
         WHERE tenant_id = $1 AND conversation_id = $2 AND status IN (1, 2, 3, 4)
         ORDER BY admitted_at_ms, run_id",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_all(pool)
    .await
    .map_err(|_| dependency(correlation_id))?
    .into_iter()
    .map(|value| RunId::from_uuid(value).map_err(|_| corrupt(correlation_id)))
    .collect::<Result<Vec<_>, _>>()?;
    let rows = sqlx::query(
        "SELECT branch.branch_id, branch.parent_branch_id,
                branch.fork_from_record_id, branch.fork_from_sequence::text,
                branch.origin, branch.edit_group_id, branch.edit_target_message_id,
                branch.created_by, branch.created_sequence::text, branch.created_at_ms,
                content.content_id AS summary_content_id,
                content.media_type AS summary_media_type,
                content.byte_length::text AS summary_byte_length,
                content.content_hash AS summary_content_hash
         FROM conversation.context_branches AS branch
         LEFT JOIN artifact.content_refs AS content
           ON content.tenant_id = branch.tenant_id
          AND content.content_id = branch.branch_summary_content_id
         WHERE branch.tenant_id = $1 AND branch.conversation_id = $2
           AND branch.created_sequence <= $3::bigint
         ORDER BY branch.created_sequence, branch.branch_id
         LIMIT 256",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(through_offset.get().to_string())
    .fetch_all(pool)
    .await
    .map_err(|_| dependency(correlation_id))?;
    let context_branches = rows
        .iter()
        .map(|row| decode_context_branch_view(row, correlation_id))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ConversationRuntimeState {
        active_activity_ids,
        active_run_ids,
        context_branches,
    })
}

#[derive(Debug, Copy, Clone)]
struct BranchProjection {
    parent_branch_id: Option<ContextBranchId>,
    fork_from_sequence: ConversationSeq,
}

impl PostgresRuntimeApplicationStore {
    pub(super) async fn authorize_edit_and_resubmit_inner(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        participant_id: musteragent_identity::ParticipantId,
        target_message_id: musteragent_identity::MessageId,
        expected_home_epoch: HomeEpoch,
        correlation_id: CorrelationId,
    ) -> Result<EditAndResubmitAuthority, RuntimeError> {
        let admission = self
            .authorize_inner(
                actor,
                conversation_id,
                participant_id,
                expected_home_epoch,
                correlation_id,
            )
            .await?;
        self.ensure_no_active_run(actor, conversation_id, correlation_id)
            .await?;
        let active_branch_id = admission.active_context_branch_id;
        let target_sequence = load_edit_target_sequence(
            &self.pool,
            actor,
            conversation_id,
            participant_id,
            target_message_id,
            correlation_id,
        )
        .await?;
        let (_, target_record_branch_id) = load_record_at_sequence(
            &self.pool,
            actor.tenant_id,
            conversation_id,
            target_sequence,
            correlation_id,
        )
        .await?;
        let branches =
            load_branches(&self.pool, actor.tenant_id, conversation_id, correlation_id).await?;
        if !is_reachable(
            &branches,
            active_branch_id,
            target_record_branch_id,
            target_sequence,
            correlation_id,
        )? {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Edited message is not reachable from the active Context branch",
            ));
        }
        ensure_edit_target_active(
            &self.pool,
            actor.tenant_id,
            conversation_id,
            target_message_id,
            &branches,
            active_branch_id,
            correlation_id,
        )
        .await?;
        let replacement_fork_sequence =
            ConversationSeq::new(target_sequence.get() - 1).map_err(|_| corrupt(correlation_id))?;
        let replacement_prior_discovery = self
            .load_prior_discovery(
                actor.tenant_id,
                conversation_id,
                admission.home_epoch,
                ReachableContextPoint {
                    lineage_head_branch_id: active_branch_id,
                    through_offset: LedgerOffset::new(replacement_fork_sequence.get()),
                },
                correlation_id,
            )
            .await?;
        let (replacement_fork_record_id, _) = load_record_at_sequence(
            &self.pool,
            actor.tenant_id,
            conversation_id,
            replacement_fork_sequence,
            correlation_id,
        )
        .await?;
        let preserved_fork_sequence = ConversationSeq::new(admission.current_offset.get())
            .map_err(|_| corrupt(correlation_id))?;
        let (preserved_fork_record_id, _) = load_record_at_sequence(
            &self.pool,
            actor.tenant_id,
            conversation_id,
            preserved_fork_sequence,
            correlation_id,
        )
        .await?;
        Ok(EditAndResubmitAuthority {
            admission,
            replacement_prior_discovery,
            active_branch_id,
            preserved_fork_record_id,
            preserved_fork_sequence,
            replacement_fork_record_id,
            replacement_fork_sequence,
        })
    }

    pub(super) async fn edit_and_resubmit_inner(
        &self,
        correlation_id: CorrelationId,
        commit: EditAndResubmitCommit,
    ) -> Result<AdmissionReceipt, RuntimeError> {
        self.edit_and_resubmit_atomic(correlation_id, commit, None)
            .await
    }

    pub(super) async fn edit_and_resubmit_content_inner(
        &self,
        correlation_id: CorrelationId,
        commit: musteragent_runtime_session::EditAndResubmitAdmissionCommit,
    ) -> Result<AdmissionReceipt, RuntimeError> {
        self.edit_and_resubmit_atomic(correlation_id, commit.edit, Some(commit.content_stage))
            .await
    }

    async fn edit_and_resubmit_atomic(
        &self,
        correlation_id: CorrelationId,
        commit: EditAndResubmitCommit,
        admission_content: Option<musteragent_runtime_content::AdmissionContentStage>,
    ) -> Result<AdmissionReceipt, RuntimeError> {
        validate_edit_and_resubmit(&commit).map_err(|code| invalid_commit(code, correlation_id))?;
        if admission_content.as_ref().is_some_and(|stage| {
            stage.tenant_id != commit.append.tenant_id
                || stage.command_fingerprint != commit.append.command_fingerprint
                || stage.content != commit.admission.message.content
        }) {
            return Err(runtime_error(
                ErrorCode::ContentIntegrityFailed,
                correlation_id,
                false,
                "Edit admission content stage does not match its product commit",
            ));
        }
        let mut prepared = match self.ledger.begin_append(&commit.append).await {
            Ok(BeginAppend::Duplicate(receipt)) => {
                verify_edit_duplicate(&self.pool, &commit, correlation_id).await?;
                if let Some(stage) = admission_content.as_ref() {
                    self.admission
                        .validate_duplicate_content_admission(&commit.admission, stage)
                        .await
                        .map_err(|error| map_admission(error, correlation_id))?;
                }
                return Ok(AdmissionReceipt {
                    message_sequence: ConversationSeq::new(
                        receipt
                            .first_sequence
                            .get()
                            .checked_add(3)
                            .ok_or_else(|| corrupt(correlation_id))?,
                    )
                    .map_err(|_| corrupt(correlation_id))?,
                    ledger: receipt,
                });
            }
            Ok(BeginAppend::Ready(prepared)) => prepared,
            Err(error) => return Err(map_ledger(error, correlation_id)),
        };
        let snapshot = self
            .admission
            .validate_new_admission_prefix(prepared.transaction_mut(), &commit.admission)
            .await
            .map_err(|error| map_admission(error, correlation_id))?;
        lock_and_validate_conversation(
            prepared.transaction_mut(),
            &commit.actor,
            commit.append.conversation_id,
            commit.append.expected_home_epoch,
            commit.parent_branch_id,
            true,
            correlation_id,
        )
        .await?;
        ensure_no_active_run_in_transaction(
            prepared.transaction_mut(),
            commit.actor,
            commit.append.conversation_id,
            correlation_id,
        )
        .await?;
        validate_admission_authority(prepared.transaction_mut(), &commit.admission)
            .await
            .map_err(|error| map_admission(error, correlation_id))?;
        let target_sequence = load_edit_target_sequence_in_transaction(
            prepared.transaction_mut(),
            commit.actor,
            commit.append.conversation_id,
            commit.admission.message.participant_id,
            commit.target_message_id,
            correlation_id,
        )
        .await?;
        let (_, target_branch_id) = load_record_at_sequence_in_transaction(
            prepared.transaction_mut(),
            commit.actor.tenant_id,
            commit.append.conversation_id,
            target_sequence,
            correlation_id,
        )
        .await?;
        let branches = load_branches_in_transaction(
            prepared.transaction_mut(),
            commit.actor.tenant_id,
            commit.append.conversation_id,
            correlation_id,
        )
        .await?;
        ensure_branch_capacity_in_transaction(
            prepared.transaction_mut(),
            commit.actor.tenant_id,
            commit.append.conversation_id,
            2,
            correlation_id,
        )
        .await?;
        if !is_reachable(
            &branches,
            commit.parent_branch_id,
            target_branch_id,
            target_sequence,
            correlation_id,
        )? {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Edited message is not reachable from the active Context branch",
            ));
        }
        ensure_edit_target_active(
            &mut *prepared.transaction_mut(),
            commit.actor.tenant_id,
            commit.append.conversation_id,
            commit.target_message_id,
            &branches,
            commit.parent_branch_id,
            correlation_id,
        )
        .await?;
        if target_sequence.get().saturating_sub(1) != commit.replacement_fork_sequence.get()
            || commit.preserved_fork_sequence.get() != commit.append.expected_offset.get()
        {
            return Err(runtime_error(
                ErrorCode::ConversationSeqConflict,
                correlation_id,
                false,
                "Conversation changed before edit-and-resubmit",
            ));
        }
        verify_record_identity_at_sequence(
            prepared.transaction_mut(),
            commit.actor.tenant_id,
            commit.append.conversation_id,
            commit.preserved_fork_sequence,
            commit.preserved_fork_record_id,
            correlation_id,
        )
        .await?;
        verify_record_identity_at_sequence(
            prepared.transaction_mut(),
            commit.actor.tenant_id,
            commit.append.conversation_id,
            commit.replacement_fork_sequence,
            commit.replacement_fork_record_id,
            correlation_id,
        )
        .await?;
        let sequences = prepared.sequences();
        let preserved_created_sequence = sequences[0];
        let replacement_created_sequence = sequences[1];
        let activated_sequence = sequences[2];
        let message_sequence = sequences[3];
        insert_context_branch(
            prepared.transaction_mut(),
            &commit,
            ContextBranchInsert {
                branch_id: commit.preserved_branch_id,
                fork_from_record_id: commit.preserved_fork_record_id,
                fork_from_sequence: commit.preserved_fork_sequence,
                created_sequence: preserved_created_sequence,
                origin: ContextBranchOrigin::EditPreserved,
            },
            correlation_id,
        )
        .await?;
        insert_context_branch(
            prepared.transaction_mut(),
            &commit,
            ContextBranchInsert {
                branch_id: commit.replacement_branch_id,
                fork_from_record_id: commit.replacement_fork_record_id,
                fork_from_sequence: commit.replacement_fork_sequence,
                created_sequence: replacement_created_sequence,
                origin: ContextBranchOrigin::EditReplacement,
            },
            correlation_id,
        )
        .await?;
        if let Some(stage) = admission_content.as_ref() {
            self.admission
                .publish_content_stage(prepared.transaction_mut(), stage)
                .await
                .map_err(|error| map_admission(error, correlation_id))?;
        }
        authorize_content(prepared.transaction_mut(), &commit.admission)
            .await
            .map_err(|error| map_admission(error, correlation_id))?;
        insert_message(
            prepared.transaction_mut(),
            &commit.admission,
            message_sequence.get(),
        )
        .await
        .map_err(|error| map_admission(error, correlation_id))?;
        insert_activity(prepared.transaction_mut(), &commit.admission)
            .await
            .map_err(|error| map_admission(error, correlation_id))?;
        consume_execution_grant(prepared.transaction_mut(), &commit.admission)
            .await
            .map_err(|error| map_admission(error, correlation_id))?;
        insert_execution_grant_routes(prepared.transaction_mut(), &commit.admission)
            .await
            .map_err(|error| map_admission(error, correlation_id))?;
        insert_memory_selections(prepared.transaction_mut(), &commit.admission)
            .await
            .map_err(|error| map_admission(error, correlation_id))?;
        insert_runtime_snapshot(prepared.transaction_mut(), &commit.admission, &snapshot)
            .await
            .map_err(|error| map_admission(error, correlation_id))?;
        insert_run(
            prepared.transaction_mut(),
            &commit.admission,
            message_sequence.get(),
        )
        .await
        .map_err(|error| map_admission(error, correlation_id))?;
        let mut staged = prepared
            .stage_with_context_branch_from(&commit.append, Some((3, commit.replacement_branch_id)))
            .await
            .map_err(|error| map_ledger(error, correlation_id))?;
        sqlx::query(
            "INSERT INTO conversation.context_branch_state (
                 tenant_id, conversation_id, active_branch_id,
                 activated_sequence, updated_at_ms
             ) VALUES ($1, $2, $3, $4::bigint, $5)
             ON CONFLICT (tenant_id, conversation_id) DO UPDATE
             SET active_branch_id = EXCLUDED.active_branch_id,
                 activated_sequence = EXCLUDED.activated_sequence,
                 updated_at_ms = EXCLUDED.updated_at_ms",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .bind(commit.replacement_branch_id.into_uuid())
        .bind(activated_sequence.get().to_string())
        .bind(commit.created_at.unix_millis())
        .execute(&mut *staged.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        let receipt = staged
            .commit()
            .await
            .map_err(|error| map_ledger(error, correlation_id))?;
        debug_assert_eq!(receipt.disposition, AppendDisposition::Committed);
        Ok(AdmissionReceipt {
            ledger: receipt,
            message_sequence,
        })
    }

    pub(super) async fn authorize_fork_context_inner(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        parent_branch_id: Option<ContextBranchId>,
        fork_from_record_id: LedgerRecordId,
        correlation_id: CorrelationId,
    ) -> Result<ForkContextAuthority, RuntimeError> {
        let context = self
            .load_context_branch_authority(actor, conversation_id, correlation_id)
            .await?;
        if context.active_branch_id != parent_branch_id {
            return Err(runtime_error(
                ErrorCode::ConversationSeqConflict,
                correlation_id,
                false,
                "Context branch parent is no longer active",
            ));
        }
        self.ensure_no_active_run(actor, conversation_id, correlation_id)
            .await?;
        let (fork_from_sequence, record_branch_id) = load_record_branch(
            &self.pool,
            actor,
            conversation_id,
            fork_from_record_id,
            correlation_id,
        )
        .await?;
        let branches =
            load_branches(&self.pool, actor.tenant_id, conversation_id, correlation_id).await?;
        if !is_reachable(
            &branches,
            context.active_branch_id,
            record_branch_id,
            fork_from_sequence,
            correlation_id,
        )? {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Fork record is not reachable from the active Context branch",
            ));
        }
        Ok(ForkContextAuthority {
            context,
            fork_from_sequence,
        })
    }

    pub(super) async fn authorize_activate_context_branch_inner(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        branch_id: ContextBranchId,
        correlation_id: CorrelationId,
    ) -> Result<ContextBranchAuthority, RuntimeError> {
        let context = self
            .load_context_branch_authority(actor, conversation_id, correlation_id)
            .await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM conversation.context_branches
                 WHERE tenant_id = $1 AND conversation_id = $2 AND branch_id = $3
             )",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(branch_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?;
        if !exists {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Context branch does not belong to the Conversation",
            ));
        }
        self.ensure_no_active_run(actor, conversation_id, correlation_id)
            .await?;
        Ok(context)
    }

    async fn load_context_branch_authority(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        correlation_id: CorrelationId,
    ) -> Result<ContextBranchAuthority, RuntimeError> {
        let row = sqlx::query(
            "SELECT conversation.lifecycle,
                    conversation.home_epoch::text AS home_epoch,
                    stream.last_sequence::text AS conversation_offset,
                    viewer.participant_id,
                    branch_state.active_branch_id
             FROM conversation.conversations AS conversation
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = conversation.tenant_id
              AND stream.conversation_id = conversation.conversation_id
             JOIN conversation.participants AS viewer
               ON viewer.tenant_id = conversation.tenant_id
              AND viewer.conversation_id = conversation.conversation_id
              AND viewer.kind = 1
              AND viewer.principal_ref = $3
              AND viewer.left_at_ms IS NULL
             LEFT JOIN conversation.context_branch_state AS branch_state
               ON branch_state.tenant_id = conversation.tenant_id
              AND branch_state.conversation_id = conversation.conversation_id
             WHERE conversation.tenant_id = $1
               AND conversation.conversation_id = $2
               AND conversation.control_home_kind = 1
               AND conversation.control_device_id IS NULL",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?
        .ok_or_else(|| {
            runtime_error(
                ErrorCode::ParticipantNotAuthorized,
                correlation_id,
                false,
                "Conversation participant is not authorized",
            )
        })?;
        if row
            .try_get::<i16, _>("lifecycle")
            .map_err(|_| dependency(correlation_id))?
            != 1
        {
            return Err(runtime_error(
                ErrorCode::ConversationArchived,
                correlation_id,
                false,
                "Conversation is archived",
            ));
        }
        Ok(ContextBranchAuthority {
            tenant_id: actor.tenant_id,
            conversation_id,
            home_epoch: HomeEpoch::new(parse_u64(
                row.try_get("home_epoch")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?)
            .map_err(|_| corrupt(correlation_id))?,
            current_offset: LedgerOffset::new(parse_u64(
                row.try_get("conversation_offset")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?),
            requested_by_participant_id: id(
                row.try_get("participant_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?,
            active_branch_id: optional_branch_id(
                row.try_get("active_branch_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?,
        })
    }

    pub(super) async fn fork_context_inner(
        &self,
        correlation_id: CorrelationId,
        commit: ForkContextCommit,
    ) -> Result<AppendReceipt, RuntimeError> {
        validate_fork(&commit).map_err(|code| invalid_commit(code, correlation_id))?;
        let mut prepared = match self.ledger.begin_append(&commit.append).await {
            Ok(BeginAppend::Duplicate(receipt)) => {
                verify_fork_duplicate(&self.pool, &commit, correlation_id).await?;
                return Ok(receipt);
            }
            Ok(BeginAppend::Ready(prepared)) => prepared,
            Err(error) => return Err(map_ledger(error, correlation_id)),
        };
        lock_and_validate_conversation(
            prepared.transaction_mut(),
            &commit.actor,
            commit.append.conversation_id,
            commit.append.expected_home_epoch,
            commit.parent_branch_id,
            true,
            correlation_id,
        )
        .await?;
        ensure_no_active_run_in_transaction(
            prepared.transaction_mut(),
            commit.actor,
            commit.append.conversation_id,
            correlation_id,
        )
        .await?;
        let (fork_sequence, record_branch_id) = load_record_branch_in_transaction(
            prepared.transaction_mut(),
            commit.actor,
            commit.append.conversation_id,
            commit.fork_from_record_id,
            correlation_id,
        )
        .await?;
        if fork_sequence != commit.fork_from_sequence {
            return Err(runtime_error(
                ErrorCode::ConversationSeqConflict,
                correlation_id,
                false,
                "Fork record changed before Context branch creation",
            ));
        }
        let branches = load_branches_in_transaction(
            prepared.transaction_mut(),
            commit.actor.tenant_id,
            commit.append.conversation_id,
            correlation_id,
        )
        .await?;
        if !is_reachable(
            &branches,
            commit.parent_branch_id,
            record_branch_id,
            fork_sequence,
            correlation_id,
        )? {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Fork record is not reachable from the active Context branch",
            ));
        }
        if let Some(summary) = &commit.branch_summary_ref {
            ensure_content(
                prepared.transaction_mut(),
                commit.actor,
                summary,
                correlation_id,
            )
            .await?;
        }
        ensure_branch_capacity_in_transaction(
            prepared.transaction_mut(),
            commit.actor.tenant_id,
            commit.append.conversation_id,
            1,
            correlation_id,
        )
        .await?;
        let created_sequence = prepared
            .sequences()
            .first()
            .copied()
            .ok_or_else(|| invalid_commit(ErrorCode::InvalidCommand, correlation_id))?;
        let inserted = sqlx::query(
            "INSERT INTO conversation.context_branches (
                 branch_id, tenant_id, conversation_id, parent_branch_id,
                 fork_from_record_id, fork_from_sequence, branch_summary_content_id,
                 created_by, created_sequence, created_at_ms, origin,
                 edit_group_id, edit_target_message_id
             ) VALUES ($1, $2, $3, $4, $5, $6::bigint, $7, $8, $9::bigint, $10,
                       1, NULL, NULL)
             ON CONFLICT (branch_id) DO NOTHING",
        )
        .bind(commit.branch_id.into_uuid())
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .bind(commit.parent_branch_id.map(ContextBranchId::into_uuid))
        .bind(commit.fork_from_record_id.into_uuid())
        .bind(commit.fork_from_sequence.get().to_string())
        .bind(
            commit
                .branch_summary_ref
                .as_ref()
                .map(|value| value.content_id.into_uuid()),
        )
        .bind(commit.actor.principal_id.into_uuid())
        .bind(created_sequence.get().to_string())
        .bind(commit.created_at.unix_millis())
        .execute(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        if inserted.rows_affected() != 1 {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Context branch identifier already exists",
            ));
        }
        prepared
            .commit(&commit.append)
            .await
            .map_err(|error| map_ledger(error, correlation_id))
    }

    pub(super) async fn activate_context_branch_inner(
        &self,
        correlation_id: CorrelationId,
        commit: ActivateContextBranchCommit,
    ) -> Result<AppendReceipt, RuntimeError> {
        validate_activate(&commit).map_err(|code| invalid_commit(code, correlation_id))?;
        let mut prepared = match self.ledger.begin_append(&commit.append).await {
            Ok(BeginAppend::Duplicate(receipt)) => return Ok(receipt),
            Ok(BeginAppend::Ready(prepared)) => prepared,
            Err(error) => return Err(map_ledger(error, correlation_id)),
        };
        lock_and_validate_conversation(
            prepared.transaction_mut(),
            &commit.actor,
            commit.append.conversation_id,
            commit.append.expected_home_epoch,
            None,
            false,
            correlation_id,
        )
        .await?;
        ensure_no_active_run_in_transaction(
            prepared.transaction_mut(),
            commit.actor,
            commit.append.conversation_id,
            correlation_id,
        )
        .await?;
        let branch_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM conversation.context_branches
                 WHERE tenant_id = $1 AND conversation_id = $2 AND branch_id = $3
             )",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .bind(commit.branch_id.into_uuid())
        .fetch_one(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        if !branch_exists {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Context branch does not belong to the Conversation",
            ));
        }
        let activated_sequence = prepared
            .sequences()
            .first()
            .copied()
            .ok_or_else(|| invalid_commit(ErrorCode::InvalidCommand, correlation_id))?;
        let mut staged = prepared
            .stage(&commit.append)
            .await
            .map_err(|error| map_ledger(error, correlation_id))?;
        sqlx::query(
            "INSERT INTO conversation.context_branch_state (
                 tenant_id, conversation_id, active_branch_id,
                 activated_sequence, updated_at_ms
             ) VALUES ($1, $2, $3, $4::bigint, $5)
             ON CONFLICT (tenant_id, conversation_id) DO UPDATE
             SET active_branch_id = EXCLUDED.active_branch_id,
                 activated_sequence = EXCLUDED.activated_sequence,
                 updated_at_ms = EXCLUDED.updated_at_ms",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .bind(commit.branch_id.into_uuid())
        .bind(activated_sequence.get().to_string())
        .bind(commit.activated_at.unix_millis())
        .execute(&mut *staged.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        staged
            .commit()
            .await
            .map_err(|error| map_ledger(error, correlation_id))
    }
}

async fn lock_and_validate_conversation(
    transaction: &mut sqlx::PgConnection,
    actor: &Actor,
    conversation_id: ConversationId,
    expected_home_epoch: HomeEpoch,
    expected_active_branch_id: Option<ContextBranchId>,
    verify_active_branch: bool,
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError> {
    let row = sqlx::query(
        "SELECT conversation.lifecycle,
                viewer.participant_id,
                branch_state.active_branch_id
         FROM conversation.conversations AS conversation
         JOIN conversation.participants AS viewer
           ON viewer.tenant_id = conversation.tenant_id
          AND viewer.conversation_id = conversation.conversation_id
          AND viewer.kind = 1
          AND viewer.principal_ref = $3
          AND viewer.left_at_ms IS NULL
         LEFT JOIN conversation.context_branch_state AS branch_state
           ON branch_state.tenant_id = conversation.tenant_id
          AND branch_state.conversation_id = conversation.conversation_id
         WHERE conversation.tenant_id = $1
           AND conversation.conversation_id = $2
           AND conversation.home_epoch = $4::bigint
           AND conversation.control_home_kind = 1
           AND conversation.control_device_id IS NULL
         FOR UPDATE OF conversation",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(expected_home_epoch.get().to_string())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|_| dependency(correlation_id))?
    .ok_or_else(|| {
        runtime_error(
            ErrorCode::ConversationEpochStale,
            correlation_id,
            false,
            "Conversation ownership or participant authority changed",
        )
    })?;
    if row
        .try_get::<i16, _>("lifecycle")
        .map_err(|_| dependency(correlation_id))?
        != 1
    {
        return Err(runtime_error(
            ErrorCode::ConversationArchived,
            correlation_id,
            false,
            "Conversation is archived",
        ));
    }
    if verify_active_branch
        && optional_branch_id(
            row.try_get("active_branch_id")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )? != expected_active_branch_id
    {
        return Err(runtime_error(
            ErrorCode::ConversationSeqConflict,
            correlation_id,
            false,
            "Context branch parent is no longer active",
        ));
    }
    Ok(())
}

async fn ensure_no_active_run_in_transaction(
    transaction: &mut sqlx::PgConnection,
    actor: Actor,
    conversation_id: ConversationId,
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError> {
    let active_runs = sqlx::query_scalar::<_, uuid::Uuid>(
        "SELECT run_id FROM runtime.runs
         WHERE tenant_id = $1 AND conversation_id = $2
           AND status IN (1, 2, 3, 4)
         FOR UPDATE",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await
    .map_err(|_| dependency(correlation_id))?;
    if !active_runs.is_empty() {
        return Err(runtime_error(
            ErrorCode::RunTransitionInvalid,
            correlation_id,
            false,
            "Conversation has an active Run",
        ));
    }
    Ok(())
}

async fn ensure_content(
    transaction: &mut sqlx::PgConnection,
    actor: Actor,
    content: &musteragent_content_ref::ContentRef,
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError> {
    let matches: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.content_refs
             WHERE tenant_id = $1 AND content_id = $2
               AND media_type = $3 AND byte_length = $4::bigint
               AND content_hash = $5 AND state = 2
         )",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(content.content_id.into_uuid())
    .bind(content.media_type.as_str())
    .bind(content.byte_length.to_string())
    .bind(content.sha256.into_bytes().to_vec())
    .fetch_one(&mut *transaction)
    .await
    .map_err(|_| dependency(correlation_id))?;
    if !matches {
        return Err(runtime_error(
            ErrorCode::InvalidCommand,
            correlation_id,
            false,
            "Context branch summary ContentRef is not authorized",
        ));
    }
    Ok(())
}

async fn load_record_branch(
    pool: &sqlx::PgPool,
    actor: Actor,
    conversation_id: ConversationId,
    record_id: LedgerRecordId,
    correlation_id: CorrelationId,
) -> Result<(ConversationSeq, Option<ContextBranchId>), RuntimeError> {
    let row = sqlx::query(
        "SELECT conversation_seq::text AS conversation_seq, context_branch_id
         FROM event_store.ledger_records
         WHERE tenant_id = $1 AND conversation_id = $2 AND record_id = $3",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(record_id.into_uuid())
    .fetch_optional(pool)
    .await
    .map_err(|_| dependency(correlation_id))?
    .ok_or_else(|| {
        runtime_error(
            ErrorCode::InvalidCommand,
            correlation_id,
            false,
            "Fork record does not belong to the Conversation",
        )
    })?;
    decode_record_branch(&row, correlation_id)
}

async fn load_record_branch_in_transaction(
    transaction: &mut sqlx::PgConnection,
    actor: Actor,
    conversation_id: ConversationId,
    record_id: LedgerRecordId,
    correlation_id: CorrelationId,
) -> Result<(ConversationSeq, Option<ContextBranchId>), RuntimeError> {
    let row = sqlx::query(
        "SELECT conversation_seq::text AS conversation_seq, context_branch_id
         FROM event_store.ledger_records
         WHERE tenant_id = $1 AND conversation_id = $2 AND record_id = $3",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(record_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|_| dependency(correlation_id))?
    .ok_or_else(|| {
        runtime_error(
            ErrorCode::InvalidCommand,
            correlation_id,
            false,
            "Fork record does not belong to the Conversation",
        )
    })?;
    decode_record_branch(&row, correlation_id)
}

fn decode_record_branch(
    row: &sqlx::postgres::PgRow,
    correlation_id: CorrelationId,
) -> Result<(ConversationSeq, Option<ContextBranchId>), RuntimeError> {
    Ok((
        ConversationSeq::new(parse_u64(
            row.try_get("conversation_seq")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?)
        .map_err(|_| corrupt(correlation_id))?,
        optional_branch_id(
            row.try_get("context_branch_id")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?,
    ))
}

async fn load_branches(
    pool: &sqlx::PgPool,
    tenant_id: musteragent_identity::TenantId,
    conversation_id: ConversationId,
    correlation_id: CorrelationId,
) -> Result<BTreeMap<ContextBranchId, BranchProjection>, RuntimeError> {
    let rows = sqlx::query(
        "SELECT branch_id, parent_branch_id, fork_from_sequence::text AS fork_from_sequence
         FROM conversation.context_branches
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_all(pool)
    .await
    .map_err(|_| dependency(correlation_id))?;
    decode_branches(rows, correlation_id)
}

async fn load_branches_in_transaction(
    transaction: &mut sqlx::PgConnection,
    tenant_id: musteragent_identity::TenantId,
    conversation_id: ConversationId,
    correlation_id: CorrelationId,
) -> Result<BTreeMap<ContextBranchId, BranchProjection>, RuntimeError> {
    let rows = sqlx::query(
        "SELECT branch_id, parent_branch_id, fork_from_sequence::text AS fork_from_sequence
         FROM conversation.context_branches
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_all(&mut *transaction)
    .await
    .map_err(|_| dependency(correlation_id))?;
    decode_branches(rows, correlation_id)
}

fn decode_branches(
    rows: Vec<sqlx::postgres::PgRow>,
    correlation_id: CorrelationId,
) -> Result<BTreeMap<ContextBranchId, BranchProjection>, RuntimeError> {
    rows.into_iter()
        .map(|row| {
            Ok((
                id(
                    row.try_get("branch_id")
                        .map_err(|_| dependency(correlation_id))?,
                    correlation_id,
                )?,
                BranchProjection {
                    parent_branch_id: optional_branch_id(
                        row.try_get("parent_branch_id")
                            .map_err(|_| dependency(correlation_id))?,
                        correlation_id,
                    )?,
                    fork_from_sequence: ConversationSeq::new(parse_u64(
                        row.try_get("fork_from_sequence")
                            .map_err(|_| dependency(correlation_id))?,
                        correlation_id,
                    )?)
                    .map_err(|_| corrupt(correlation_id))?,
                },
            ))
        })
        .collect()
}

fn is_reachable(
    branches: &BTreeMap<ContextBranchId, BranchProjection>,
    active_branch_id: Option<ContextBranchId>,
    record_branch_id: Option<ContextBranchId>,
    record_sequence: ConversationSeq,
    correlation_id: CorrelationId,
) -> Result<bool, RuntimeError> {
    let mut current = active_branch_id;
    let mut maximum_sequence = None;
    loop {
        if current == record_branch_id
            && maximum_sequence.is_none_or(|limit: ConversationSeq| record_sequence <= limit)
        {
            return Ok(true);
        }
        let Some(branch_id) = current else {
            return Ok(false);
        };
        let branch = branches.get(&branch_id).ok_or_else(|| {
            runtime_error(
                ErrorCode::InternalInvariantViolated,
                correlation_id,
                false,
                "Context branch projection is corrupt",
            )
        })?;
        current = branch.parent_branch_id;
        maximum_sequence = Some(maximum_sequence.map_or(branch.fork_from_sequence, |limit| {
            std::cmp::min(limit, branch.fork_from_sequence)
        }));
    }
}

async fn verify_fork_duplicate(
    pool: &sqlx::PgPool,
    commit: &ForkContextCommit,
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError> {
    let matches: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM conversation.context_branches
             WHERE branch_id = $1 AND tenant_id = $2 AND conversation_id = $3
               AND parent_branch_id IS NOT DISTINCT FROM $4
               AND fork_from_record_id = $5
               AND fork_from_sequence = $6::bigint
               AND branch_summary_content_id IS NOT DISTINCT FROM $7
               AND created_by = $8 AND created_at_ms = $9
         )",
    )
    .bind(commit.branch_id.into_uuid())
    .bind(commit.actor.tenant_id.into_uuid())
    .bind(commit.append.conversation_id.into_uuid())
    .bind(commit.parent_branch_id.map(ContextBranchId::into_uuid))
    .bind(commit.fork_from_record_id.into_uuid())
    .bind(commit.fork_from_sequence.get().to_string())
    .bind(
        commit
            .branch_summary_ref
            .as_ref()
            .map(|value| value.content_id.into_uuid()),
    )
    .bind(commit.actor.principal_id.into_uuid())
    .bind(commit.created_at.unix_millis())
    .fetch_one(pool)
    .await
    .map_err(|_| dependency(correlation_id))?;
    if !matches {
        return Err(runtime_error(
            ErrorCode::InternalInvariantViolated,
            correlation_id,
            false,
            "Idempotent Context branch projection does not match its Ledger commit",
        ));
    }
    Ok(())
}

fn validate_fork(commit: &ForkContextCommit) -> Result<(), ErrorCode> {
    if commit.actor.tenant_id != commit.append.tenant_id
        || !commit.append.outbox.is_empty()
        || commit.append.records.len() != 1
    {
        return Err(ErrorCode::InvalidCommand);
    }
    Ok(())
}

fn validate_activate(commit: &ActivateContextBranchCommit) -> Result<(), ErrorCode> {
    if commit.actor.tenant_id != commit.append.tenant_id
        || !commit.append.outbox.is_empty()
        || commit.append.records.len() != 1
    {
        return Err(ErrorCode::InvalidCommand);
    }
    Ok(())
}

fn invalid_commit(code: ErrorCode, correlation_id: CorrelationId) -> RuntimeError {
    runtime_error(
        code,
        correlation_id,
        false,
        "Context branch commit is invalid",
    )
}

fn optional_branch_id(
    value: Option<uuid::Uuid>,
    correlation_id: CorrelationId,
) -> Result<Option<ContextBranchId>, RuntimeError> {
    value
        .map(|value| id(value, correlation_id))
        .transpose()
        .map_err(RuntimeError::from)
}

async fn ensure_edit_target_active<'e, E>(
    executor: E,
    tenant_id: musteragent_identity::TenantId,
    conversation_id: ConversationId,
    target_message_id: musteragent_identity::MessageId,
    branches: &BTreeMap<ContextBranchId, BranchProjection>,
    active_branch_id: Option<ContextBranchId>,
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        "SELECT message.lifecycle,
                record.conversation_seq::text AS retracted_sequence,
                record.context_branch_id
         FROM conversation.messages AS message
         LEFT JOIN conversation.message_retractions AS retraction
           ON retraction.tenant_id = message.tenant_id
          AND retraction.conversation_id = message.conversation_id
          AND retraction.message_id = message.message_id
         LEFT JOIN event_store.ledger_records AS record
           ON record.record_id = retraction.retraction_record_id
         WHERE message.tenant_id = $1
           AND message.conversation_id = $2
           AND message.message_id = $3",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(target_message_id.into_uuid())
    .fetch_all(executor)
    .await
    .map_err(|_| dependency(correlation_id))?;
    let lifecycle = rows
        .first()
        .ok_or_else(|| corrupt(correlation_id))?
        .try_get::<i16, _>("lifecycle")
        .map_err(|_| dependency(correlation_id))?;
    let mut has_retraction_projection = false;
    for row in rows {
        let sequence = row
            .try_get::<Option<String>, _>("retracted_sequence")
            .map_err(|_| dependency(correlation_id))?;
        let Some(sequence) = sequence else {
            continue;
        };
        has_retraction_projection = true;
        let sequence = ConversationSeq::new(parse_u64(sequence, correlation_id)?)
            .map_err(|_| corrupt(correlation_id))?;
        let branch_id = optional_branch_id(
            row.try_get("context_branch_id")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?;
        if is_reachable(
            branches,
            active_branch_id,
            branch_id,
            sequence,
            correlation_id,
        )? {
            return Err(edit_target_retracted(correlation_id));
        }
    }
    if lifecycle == 3 && !has_retraction_projection {
        return Err(edit_target_retracted(correlation_id));
    }
    Ok(())
}

fn edit_target_retracted(correlation_id: CorrelationId) -> RuntimeError {
    runtime_error(
        ErrorCode::InvalidCommand,
        correlation_id,
        false,
        "Edited message is retracted on the active Context branch",
    )
}

async fn load_edit_target_sequence(
    pool: &sqlx::PgPool,
    actor: Actor,
    conversation_id: ConversationId,
    participant_id: musteragent_identity::ParticipantId,
    target_message_id: musteragent_identity::MessageId,
    correlation_id: CorrelationId,
) -> Result<ConversationSeq, RuntimeError> {
    let sequence: Option<String> = sqlx::query_scalar(
        "SELECT committed_sequence::text FROM conversation.messages
         WHERE tenant_id = $1 AND conversation_id = $2
           AND participant_id = $3 AND message_id = $4
           AND role = 1",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(participant_id.into_uuid())
    .bind(target_message_id.into_uuid())
    .fetch_optional(pool)
    .await
    .map_err(|_| dependency(correlation_id))?;
    decode_edit_target_sequence(sequence, correlation_id)
}

async fn load_edit_target_sequence_in_transaction(
    transaction: &mut sqlx::PgConnection,
    actor: Actor,
    conversation_id: ConversationId,
    participant_id: musteragent_identity::ParticipantId,
    target_message_id: musteragent_identity::MessageId,
    correlation_id: CorrelationId,
) -> Result<ConversationSeq, RuntimeError> {
    let sequence: Option<String> = sqlx::query_scalar(
        "SELECT committed_sequence::text FROM conversation.messages
         WHERE tenant_id = $1 AND conversation_id = $2
           AND participant_id = $3 AND message_id = $4
           AND role = 1
         FOR UPDATE",
    )
    .bind(actor.tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(participant_id.into_uuid())
    .bind(target_message_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|_| dependency(correlation_id))?;
    decode_edit_target_sequence(sequence, correlation_id)
}

fn decode_edit_target_sequence(
    sequence: Option<String>,
    correlation_id: CorrelationId,
) -> Result<ConversationSeq, RuntimeError> {
    let sequence = sequence.ok_or_else(|| {
        runtime_error(
            ErrorCode::InvalidCommand,
            correlation_id,
            false,
            "Edited message is not an active User Message on this Conversation",
        )
    })?;
    let value = parse_u64(sequence, correlation_id)?;
    if value <= 1 {
        return Err(corrupt(correlation_id));
    }
    ConversationSeq::new(value).map_err(|_| corrupt(correlation_id))
}

async fn load_record_at_sequence(
    pool: &sqlx::PgPool,
    tenant_id: musteragent_identity::TenantId,
    conversation_id: ConversationId,
    sequence: ConversationSeq,
    correlation_id: CorrelationId,
) -> Result<(LedgerRecordId, Option<ContextBranchId>), RuntimeError> {
    let row = sqlx::query(
        "SELECT record_id, context_branch_id FROM event_store.ledger_records
         WHERE tenant_id = $1 AND conversation_id = $2
           AND conversation_seq = $3::bigint",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(sequence.get().to_string())
    .fetch_optional(pool)
    .await
    .map_err(|_| dependency(correlation_id))?
    .ok_or_else(|| corrupt(correlation_id))?;
    decode_record_at_sequence(&row, correlation_id)
}

async fn load_record_at_sequence_in_transaction(
    transaction: &mut sqlx::PgConnection,
    tenant_id: musteragent_identity::TenantId,
    conversation_id: ConversationId,
    sequence: ConversationSeq,
    correlation_id: CorrelationId,
) -> Result<(LedgerRecordId, Option<ContextBranchId>), RuntimeError> {
    let row = sqlx::query(
        "SELECT record_id, context_branch_id FROM event_store.ledger_records
         WHERE tenant_id = $1 AND conversation_id = $2
           AND conversation_seq = $3::bigint",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(sequence.get().to_string())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|_| dependency(correlation_id))?
    .ok_or_else(|| corrupt(correlation_id))?;
    decode_record_at_sequence(&row, correlation_id)
}

fn decode_record_at_sequence(
    row: &sqlx::postgres::PgRow,
    correlation_id: CorrelationId,
) -> Result<(LedgerRecordId, Option<ContextBranchId>), RuntimeError> {
    Ok((
        id(
            row.try_get("record_id")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?,
        optional_branch_id(
            row.try_get("context_branch_id")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?,
    ))
}

async fn verify_record_identity_at_sequence(
    transaction: &mut sqlx::PgConnection,
    tenant_id: musteragent_identity::TenantId,
    conversation_id: ConversationId,
    sequence: ConversationSeq,
    expected_record_id: LedgerRecordId,
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError> {
    let (record_id, _) = load_record_at_sequence_in_transaction(
        transaction,
        tenant_id,
        conversation_id,
        sequence,
        correlation_id,
    )
    .await?;
    if record_id != expected_record_id {
        return Err(runtime_error(
            ErrorCode::ConversationSeqConflict,
            correlation_id,
            false,
            "Context branch anchor changed before commit",
        ));
    }
    Ok(())
}

struct ContextBranchInsert {
    branch_id: ContextBranchId,
    fork_from_record_id: LedgerRecordId,
    fork_from_sequence: ConversationSeq,
    created_sequence: ConversationSeq,
    origin: ContextBranchOrigin,
}

async fn insert_context_branch(
    transaction: &mut sqlx::PgConnection,
    commit: &EditAndResubmitCommit,
    branch: ContextBranchInsert,
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError> {
    let inserted = sqlx::query(
        "INSERT INTO conversation.context_branches (
             branch_id, tenant_id, conversation_id, parent_branch_id,
             fork_from_record_id, fork_from_sequence, branch_summary_content_id,
             created_by, created_sequence, created_at_ms, origin,
             edit_group_id, edit_target_message_id
         ) VALUES ($1, $2, $3, $4, $5, $6::bigint, NULL, $7, $8::bigint, $9,
                   $10, $11, $12)
         ON CONFLICT (branch_id) DO NOTHING",
    )
    .bind(branch.branch_id.into_uuid())
    .bind(commit.actor.tenant_id.into_uuid())
    .bind(commit.append.conversation_id.into_uuid())
    .bind(commit.parent_branch_id.map(ContextBranchId::into_uuid))
    .bind(branch.fork_from_record_id.into_uuid())
    .bind(branch.fork_from_sequence.get().to_string())
    .bind(commit.actor.principal_id.into_uuid())
    .bind(branch.created_sequence.get().to_string())
    .bind(commit.created_at.unix_millis())
    .bind(context_branch_origin_code(branch.origin))
    .bind(commit.edit_group_id.into_uuid())
    .bind(commit.target_message_id.into_uuid())
    .execute(&mut *transaction)
    .await
    .map_err(|_| dependency(correlation_id))?;
    if inserted.rows_affected() != 1 {
        return Err(runtime_error(
            ErrorCode::InvalidCommand,
            correlation_id,
            false,
            "Context branch identifier already exists",
        ));
    }
    Ok(())
}

fn validate_edit_and_resubmit(commit: &EditAndResubmitCommit) -> Result<(), ErrorCode> {
    commit
        .admission
        .validate()
        .map_err(|_| ErrorCode::InvalidCommand)?;
    if commit.preserved_branch_id == commit.replacement_branch_id
        || commit.actor != commit.admission.actor
        || commit.actor.tenant_id != commit.append.tenant_id
        || commit.append.records.len() != 6
        || commit.append.records[3..] != commit.admission.append.records
        || commit.append.outbox != commit.admission.append.outbox
        || commit.append.tenant_id != commit.admission.append.tenant_id
        || commit.append.conversation_id != commit.admission.append.conversation_id
        || commit.append.expected_home_epoch != commit.admission.append.expected_home_epoch
        || commit.append.expected_offset != commit.admission.append.expected_offset
        || commit.append.idempotency_key != commit.admission.append.idempotency_key
    {
        return Err(ErrorCode::InvalidCommand);
    }
    Ok(())
}

async fn verify_edit_duplicate(
    pool: &sqlx::PgPool,
    commit: &EditAndResubmitCommit,
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError> {
    let branch_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM conversation.context_branches
         WHERE tenant_id = $1 AND conversation_id = $2
           AND branch_id = ANY($3)
           AND edit_group_id = $4 AND edit_target_message_id = $5
           AND origin IN (2, 3)",
    )
    .bind(commit.actor.tenant_id.into_uuid())
    .bind(commit.append.conversation_id.into_uuid())
    .bind(vec![
        commit.preserved_branch_id.into_uuid(),
        commit.replacement_branch_id.into_uuid(),
    ])
    .bind(commit.edit_group_id.into_uuid())
    .bind(commit.target_message_id.into_uuid())
    .fetch_one(pool)
    .await
    .map_err(|_| dependency(correlation_id))?;
    let run_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM runtime.runs
             WHERE tenant_id = $1 AND conversation_id = $2 AND run_id = $3
         )",
    )
    .bind(commit.actor.tenant_id.into_uuid())
    .bind(commit.append.conversation_id.into_uuid())
    .bind(commit.admission.run.context.run_id.into_uuid())
    .fetch_one(pool)
    .await
    .map_err(|_| dependency(correlation_id))?;
    if branch_count != 2 || !run_exists {
        return Err(runtime_error(
            ErrorCode::InternalInvariantViolated,
            correlation_id,
            false,
            "Idempotent edit-and-resubmit projections do not match the Ledger commit",
        ));
    }
    Ok(())
}

fn context_branch_origin_code(origin: ContextBranchOrigin) -> i16 {
    match origin {
        // New command paths never construct this replay-only value. Binding
        // zero intentionally lets the database CHECK constraint fail closed
        // if that invariant is ever violated.
        ContextBranchOrigin::LegacyUnclassified => 0,
        ContextBranchOrigin::Fork => 1,
        ContextBranchOrigin::EditPreserved => 2,
        ContextBranchOrigin::EditReplacement => 3,
    }
}

fn decode_context_branch_view(
    row: &sqlx::postgres::PgRow,
    correlation_id: CorrelationId,
) -> Result<ContextBranchView, RuntimeError> {
    let branch_summary_ref = match (
        row.try_get::<Option<uuid::Uuid>, _>("summary_content_id")
            .map_err(|_| corrupt(correlation_id))?,
        row.try_get::<Option<String>, _>("summary_media_type")
            .map_err(|_| corrupt(correlation_id))?,
        row.try_get::<Option<String>, _>("summary_byte_length")
            .map_err(|_| corrupt(correlation_id))?,
        row.try_get::<Option<Vec<u8>>, _>("summary_content_hash")
            .map_err(|_| corrupt(correlation_id))?,
    ) {
        (None, None, None, None) => None,
        (Some(content_id), Some(media_type), Some(byte_length), Some(content_hash)) => {
            Some(ContentRef {
                content_id: ContentId::from_uuid(content_id)
                    .map_err(|_| corrupt(correlation_id))?,
                media_type: MediaType::new(media_type).map_err(|_| corrupt(correlation_id))?,
                byte_length: parse_u64(byte_length, correlation_id)?,
                sha256: Sha256::from_bytes(content_hash).map_err(|_| corrupt(correlation_id))?,
            })
        }
        _ => return Err(corrupt(correlation_id)),
    };
    let origin = match row
        .try_get::<i16, _>("origin")
        .map_err(|_| corrupt(correlation_id))?
    {
        1 => ContextBranchOrigin::Fork,
        2 => ContextBranchOrigin::EditPreserved,
        3 => ContextBranchOrigin::EditReplacement,
        _ => return Err(corrupt(correlation_id)),
    };
    Ok(ContextBranchView {
        branch_id: ContextBranchId::from_uuid(
            row.try_get("branch_id")
                .map_err(|_| corrupt(correlation_id))?,
        )
        .map_err(|_| corrupt(correlation_id))?,
        parent_branch_id: row
            .try_get::<Option<uuid::Uuid>, _>("parent_branch_id")
            .map_err(|_| corrupt(correlation_id))?
            .map(ContextBranchId::from_uuid)
            .transpose()
            .map_err(|_| corrupt(correlation_id))?,
        fork_from_record_id: LedgerRecordId::from_uuid(
            row.try_get("fork_from_record_id")
                .map_err(|_| corrupt(correlation_id))?,
        )
        .map_err(|_| corrupt(correlation_id))?,
        fork_from_sequence: ConversationSeq::new(parse_u64(
            row.try_get("fork_from_sequence")
                .map_err(|_| corrupt(correlation_id))?,
            correlation_id,
        )?)
        .map_err(|_| corrupt(correlation_id))?,
        branch_summary_ref,
        origin,
        edit_group_id: row
            .try_get::<Option<uuid::Uuid>, _>("edit_group_id")
            .map_err(|_| corrupt(correlation_id))?
            .map(CorrelationId::from_uuid)
            .transpose()
            .map_err(|_| corrupt(correlation_id))?,
        edit_target_message_id: row
            .try_get::<Option<uuid::Uuid>, _>("edit_target_message_id")
            .map_err(|_| corrupt(correlation_id))?
            .map(MessageId::from_uuid)
            .transpose()
            .map_err(|_| corrupt(correlation_id))?,
        created_by: PrincipalId::from_uuid(
            row.try_get("created_by")
                .map_err(|_| corrupt(correlation_id))?,
        )
        .map_err(|_| corrupt(correlation_id))?,
        created_sequence: ConversationSeq::new(parse_u64(
            row.try_get("created_sequence")
                .map_err(|_| corrupt(correlation_id))?,
            correlation_id,
        )?)
        .map_err(|_| corrupt(correlation_id))?,
        created_at: UtcTimestamp::from_unix_millis(
            row.try_get("created_at_ms")
                .map_err(|_| corrupt(correlation_id))?,
        ),
    })
}

async fn ensure_branch_capacity_in_transaction(
    transaction: &mut sqlx::PgConnection,
    tenant_id: musteragent_identity::TenantId,
    conversation_id: ConversationId,
    requested: i64,
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError> {
    let existing: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM conversation.context_branches
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_one(&mut *transaction)
    .await
    .map_err(|_| dependency(correlation_id))?;
    if existing.saturating_add(requested) > MAX_CONTEXT_BRANCHES_PER_CONVERSATION {
        return Err(runtime_error(
            ErrorCode::QuotaExceeded,
            correlation_id,
            false,
            "Conversation Context branch limit reached",
        ));
    }
    Ok(())
}
