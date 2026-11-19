use std::collections::BTreeSet;

use musteragent_activity::ActivityKind;
use musteragent_content_ref::Sha256;
use musteragent_context::RuntimeSnapshotDecoder;
use musteragent_conversation::{
    Conversation, ConversationKind, ConversationLifecycle, ConversationListCursor,
    ConversationOperationalSummary, ConversationPage, ConversationPendingInteractionKind,
    ConversationRunWaitReason, ConversationSummary, ConversationView, ListConversations,
    Participant, ParticipantDisplaySnapshot, ParticipantIdentity, ParticipantKind, ServiceRef,
};
use musteragent_event_envelope::{ExecutionScope, LedgerCursor, Producer, RuntimeHome};
use musteragent_identity::{
    ActivityId, Actor, AgentId, AttemptId, AttemptNumber, CommandId, ContextBranchId,
    ConversationId, ConversationSeq, CorrelationId, DelegationId, DeviceId, FencingToken,
    HomeEpoch, InteractionId, LeaseId, LedgerOffset, LedgerRecordId, MessageId, ParticipantId,
    PrincipalId, RunId, RuntimeSnapshotId, TenantId, UtcTimestamp, WorkspaceId,
};
use musteragent_ledger::{
    AdmissionBatch, AdmissionReceipt, AppendDisposition, CancelMode, CancelRunBatch,
    CancelRunReceipt, RetryAdmissionBatch, RunControlBatch,
};
use musteragent_run::{
    AttemptContext, Run, RunContext, RunState, RunStatus, RunView, RunWaitReason, WorkerId,
};
use musteragent_runtime_contract::{
    ApplicationFuture, CommandAck, CommandDisposition, DisplayText, ErrorCode, ErrorInfo,
    ResourceRef, RuntimeError, SafeDisplayText,
};
use musteragent_runtime_session::{
    ActivateContextBranchCommit, AdmissionAuditContext, AdmissionAuthority, AdmissionCommit,
    AdmissionReplayReceipt, AgentAdmissionIdentity, ArchiveConversationAuthority,
    ArchiveConversationCommit, ArchiveRunTarget, CancelRunAuthority, ContextBranchAuthority,
    ConversationCreationRecoveryQuery, CreateConversationCommit, EditAndResubmitAdmissionCommit,
    EditAndResubmitAuthority, EditAndResubmitCommit, ForkContextAuthority, ForkContextCommit,
    RetractMessageAuthority, RetractMessageCommit, RunControlAuthority, RunControlCommit,
    RunDiscoveryQuery, RuntimeApplicationStore, SetConversationTitleAuthority,
    SetConversationTitleCommit, StandingGoalAdmissionCommit, runtime_error,
};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row as _};

use crate::PostgresSecurityAuditPipeline;
use crate::admission::PostgresAdmissionStore;
use crate::catalog::PostgresAgentCatalog;
use crate::content::PostgresContentStore;
use crate::execution::{CancelMutation, apply_cancel_mutation};
use crate::ledger::{BeginAppend, PostgresLedgerStore};
use crate::runtime_application_error::{
    corrupt, database_dependency, dependency, map_admission, map_execution, map_ledger,
    map_run_control, not_found,
};

mod admission_denial;
mod cancellation_authority;
mod context_branch;
mod context_reachability;
mod creation_recovery;
mod dialogue_control;
mod idempotency;
mod prior_discovery;
mod recovery;
mod retraction;
mod support;
mod work_item_control;
use context_reachability::ReachableContextPoint;
use support::*;

fn decode_conversation_operational_summary(
    row: &PgRow,
    correlation_id: CorrelationId,
) -> Result<ConversationOperationalSummary, RuntimeError> {
    let count = |column| {
        row.try_get::<i64, _>(column)
            .map_err(|_| dependency(correlation_id))
            .and_then(|value| u32::try_from(value).map_err(|_| corrupt(correlation_id)))
    };
    let mut wait_reasons = Vec::new();
    for (column, reason) in [
        ("wait_approval_count", ConversationRunWaitReason::Approval),
        ("wait_input_count", ConversationRunWaitReason::Input),
        ("wait_device_count", ConversationRunWaitReason::Device),
        (
            "wait_retry_backoff_count",
            ConversationRunWaitReason::RetryBackoff,
        ),
        (
            "wait_delegation_count",
            ConversationRunWaitReason::Delegation,
        ),
        (
            "wait_external_operation_count",
            ConversationRunWaitReason::ExternalOperation,
        ),
    ] {
        if count(column)? > 0 {
            wait_reasons.push(reason);
        }
    }
    let mut pending_interaction_kinds = Vec::new();
    for (column, kind) in [
        (
            "pending_approval_count",
            ConversationPendingInteractionKind::Approval,
        ),
        (
            "pending_clarification_count",
            ConversationPendingInteractionKind::Clarification,
        ),
        (
            "pending_confirmation_count",
            ConversationPendingInteractionKind::Confirmation,
        ),
        (
            "pending_selection_count",
            ConversationPendingInteractionKind::Selection,
        ),
        (
            "pending_credential_count",
            ConversationPendingInteractionKind::Credential,
        ),
        (
            "pending_oauth_count",
            ConversationPendingInteractionKind::OAuth,
        ),
    ] {
        if count(column)? > 0 {
            pending_interaction_kinds.push(kind);
        }
    }
    Ok(ConversationOperationalSummary {
        active_run_count: count("active_run_count")?,
        queued_run_count: count("queued_run_count")?,
        running_run_count: count("running_run_count")?,
        waiting_run_count: count("waiting_run_count")?,
        cancelling_run_count: count("cancelling_run_count")?,
        wait_reasons,
        pending_interaction_count: count("pending_interaction_count")?,
        pending_interaction_kinds,
    })
}

#[derive(Clone)]
pub struct PostgresRuntimeApplicationStore {
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    admission: PostgresAdmissionStore,
    ledger: PostgresLedgerStore,
    catalog: PostgresAgentCatalog,
    snapshots: std::sync::Arc<dyn RuntimeSnapshotDecoder>,
    application_catalogs: Option<
        std::sync::Arc<
            dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
        >,
    >,
    audit: std::sync::Arc<PostgresSecurityAuditPipeline>,
    content: Option<PostgresContentStore>,
}

impl PostgresRuntimeApplicationStore {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        snapshots: std::sync::Arc<dyn RuntimeSnapshotDecoder>,
    ) -> Self {
        let database = database.into();
        let audit = std::sync::Arc::new(PostgresSecurityAuditPipeline::standard(database.clone()));
        Self::with_security_audit(database, snapshots, audit)
    }

    #[must_use]
    pub fn with_security_audit(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        snapshots: std::sync::Arc<dyn RuntimeSnapshotDecoder>,
        audit: std::sync::Arc<PostgresSecurityAuditPipeline>,
    ) -> Self {
        let database = database.into();
        let pool = database.pool().clone();
        Self {
            admission: PostgresAdmissionStore::with_security_audit(
                database.clone(),
                std::sync::Arc::clone(&snapshots),
                std::sync::Arc::clone(&audit),
            ),
            ledger: PostgresLedgerStore::new(database.clone()),
            catalog: PostgresAgentCatalog::new(pool.clone()),
            snapshots,
            application_catalogs: None,
            audit,
            content: None,
            pool,
            database,
        }
    }

    #[must_use]
    pub fn with_admission_content_store(mut self, content: PostgresContentStore) -> Self {
        self.admission = self.admission.with_content_store(content.clone());
        self.content = Some(content);
        self
    }

    #[must_use]
    pub fn with_application_catalogs(
        mut self,
        source: Option<std::sync::Arc<dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource>>,
    ) -> Self {
        self.application_catalogs = source;
        self
    }

    async fn enforce_foreground_priority(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        activity_kind: ActivityKind,
        correlation_id: CorrelationId,
    ) -> Result<(), RuntimeError> {
        if !matches!(
            activity_kind,
            ActivityKind::Automation | ActivityKind::Evaluation
        ) {
            return Ok(());
        }
        let foreground_chat_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM runtime.runs
                 WHERE tenant_id = $1 AND conversation_id = $2
                   AND activity_kind = 1 AND status IN (1, 2, 3, 4)
             )",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_one(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?;
        if foreground_chat_exists {
            return Err(runtime_error(
                ErrorCode::ConversationSeqConflict,
                correlation_id,
                true,
                "A foreground user turn preempted the autonomous Goal admission",
            ));
        }
        Ok(())
    }

    async fn create_inner(
        &self,
        command_id: CommandId,
        correlation_id: CorrelationId,
        commit: CreateConversationCommit,
    ) -> Result<CommandAck, RuntimeError> {
        validate_create(&commit).map_err(|code| {
            runtime_error(
                code,
                correlation_id,
                false,
                "Create Conversation commit is invalid",
            )
        })?;
        let mut prepared = match self.ledger.begin_append(&commit.append).await {
            Ok(BeginAppend::Duplicate(receipt)) => {
                verify_create_duplicate(&self.pool, &commit, correlation_id).await?;
                return Ok(create_ack(command_id, &commit, receipt));
            }
            Ok(BeginAppend::Ready(prepared)) => prepared,
            Err(error) => return Err(map_ledger(error, correlation_id)),
        };
        insert_conversation(prepared.transaction_mut(), &commit)
            .await
            .map_err(|_| dependency(correlation_id))?;
        insert_participant(
            prepared.transaction_mut(),
            commit.actor.tenant_id,
            &commit.user_participant,
        )
        .await
        .map_err(|_| dependency(correlation_id))?;
        insert_participant(
            prepared.transaction_mut(),
            commit.actor.tenant_id,
            &commit.assistant_participant,
        )
        .await
        .map_err(|_| dependency(correlation_id))?;
        let receipt = prepared
            .commit(&commit.append)
            .await
            .map_err(|error| map_ledger(error, correlation_id))?;
        Ok(create_ack(command_id, &commit, receipt))
    }

    async fn authorize_inner(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        expected_home_epoch: HomeEpoch,
        correlation_id: CorrelationId,
    ) -> Result<AdmissionAuthority, RuntimeError> {
        let row = sqlx::query(
            "SELECT conversation.tenant_id,
                    conversation.home_epoch::text AS home_epoch,
                    conversation.lifecycle,
                    conversation.title,
                    stream.last_sequence::text AS last_sequence,
                    branch_state.active_branch_id,
                    EXISTS(
                        SELECT 1 FROM conversation.messages AS prior_message
                        WHERE prior_message.tenant_id = conversation.tenant_id
                          AND prior_message.conversation_id = conversation.conversation_id
                          AND prior_message.role = 1
                    ) AS has_user_messages,
                    user_participant.participant_id AS user_participant_id,
                    executor_participant.participant_id AS executor_participant_id
             FROM conversation.conversations AS conversation
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = conversation.tenant_id
              AND stream.conversation_id = conversation.conversation_id
             LEFT JOIN conversation.context_branch_state AS branch_state
               ON branch_state.tenant_id = conversation.tenant_id
              AND branch_state.conversation_id = conversation.conversation_id
             JOIN conversation.participants AS user_participant
               ON user_participant.tenant_id = conversation.tenant_id
              AND user_participant.conversation_id = conversation.conversation_id
              AND user_participant.participant_id = $4
              AND user_participant.kind = 1
              AND user_participant.principal_ref = $3
              AND user_participant.left_at_ms IS NULL
             JOIN conversation.participants AS executor_participant
               ON executor_participant.tenant_id = conversation.tenant_id
              AND executor_participant.conversation_id = conversation.conversation_id
              AND executor_participant.participant_id =
                  conversation.execution_owner_participant_id
              AND executor_participant.kind = 2
              AND executor_participant.left_at_ms IS NULL
             JOIN agent.agent_definitions AS definition
               ON definition.agent_id = executor_participant.agent_ref
              AND definition.lifecycle = 1
             WHERE conversation.purpose = 'standard' AND conversation.tenant_id = $1
               AND conversation.conversation_id = $2
               AND conversation.kind = 1
               AND conversation.control_home_kind = 1",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(participant_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            database_dependency(error, "admission.authorize_conversation", correlation_id)
        })?
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
        let home_epoch = HomeEpoch::new(parse_u64(
            row.try_get("home_epoch")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?)
        .map_err(|_| corrupt(correlation_id))?;
        if home_epoch != expected_home_epoch {
            return Err(runtime_error(
                ErrorCode::ConversationEpochStale,
                correlation_id,
                false,
                "Conversation ownership epoch is stale",
            ));
        }
        let current_offset = LedgerOffset::new(parse_u64(
            row.try_get("last_sequence")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?);
        let active_context_branch_id = row
            .try_get::<Option<uuid::Uuid>, _>("active_branch_id")
            .map_err(|_| dependency(correlation_id))?
            .map(|value| id(value, correlation_id))
            .transpose()?;
        let prior_discovery = self
            .load_prior_discovery(
                actor.tenant_id,
                conversation_id,
                home_epoch,
                ReachableContextPoint {
                    lineage_head_branch_id: active_context_branch_id,
                    through_offset: current_offset,
                },
                correlation_id,
            )
            .await?;
        Ok(AdmissionAuthority {
            tenant_id: actor.tenant_id,
            conversation_id,
            home_epoch,
            current_offset,
            title: row
                .try_get::<Option<String>, _>("title")
                .map_err(|_| dependency(correlation_id))?
                .filter(|value| !value.is_empty())
                .map(DisplayText::new)
                .transpose()
                .map_err(|_| corrupt(correlation_id))?,
            has_user_messages: row
                .try_get("has_user_messages")
                .map_err(|_| dependency(correlation_id))?,
            active_context_branch_id,
            prior_discovery,
            requested_by_participant_id: id(
                row.try_get("user_participant_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?,
            executor_participant_id: id(
                row.try_get("executor_participant_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?,
        })
    }

    async fn conversation_inner(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
    ) -> Result<ConversationView, RuntimeError> {
        let correlation_id = CorrelationId::generate();
        authorize_conversation(&self.pool, actor, conversation_id, correlation_id).await?;
        let row = sqlx::query(
            "SELECT conversation.conversation_id, conversation.tenant_id,
                    conversation.workspace_id,
                    conversation.execution_owner_participant_id,
                    conversation.kind, conversation.title,
                    conversation.control_home_kind, conversation.home_epoch::text AS home_epoch,
                    conversation.lifecycle, conversation.created_by,
                    conversation.created_at_ms, conversation.updated_at_ms,
                    conversation.archived_at_ms,
                    stream.last_sequence::text AS last_sequence,
                    branch_state.active_branch_id
             FROM conversation.conversations AS conversation
             JOIN event_store.ledger_streams AS stream
              ON stream.tenant_id = conversation.tenant_id
              AND stream.conversation_id = conversation.conversation_id
             LEFT JOIN conversation.context_branch_state AS branch_state
               ON branch_state.tenant_id = conversation.tenant_id
              AND branch_state.conversation_id = conversation.conversation_id
             WHERE conversation.purpose = 'standard' AND conversation.tenant_id = $1 AND conversation.conversation_id = $2",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?
        .ok_or_else(|| not_found(ErrorCode::ConversationNotFound, correlation_id))?;
        let conversation = decode_conversation(&row, correlation_id).map_err(RuntimeError::from)?;
        let participants =
            load_participants(&self.pool, actor.tenant_id, conversation_id, correlation_id)
                .await
                .map_err(RuntimeError::from)?;
        validate_execution_owner(&conversation, &participants, correlation_id)?;
        let offset = LedgerOffset::new(parse_u64(
            row.try_get("last_sequence")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?);
        let active_context_branch_id = row
            .try_get::<Option<uuid::Uuid>, _>("active_branch_id")
            .map_err(|_| dependency(correlation_id))?
            .map(|value| id(value, correlation_id))
            .transpose()?;
        let runtime_state = context_branch::load_conversation_runtime_state(
            &self.pool,
            actor,
            conversation_id,
            offset,
            correlation_id,
        )
        .await?;
        Ok(ConversationView {
            latest_cursor: LedgerCursor {
                conversation_id,
                home_epoch: conversation.home_epoch,
                after_conversation_seq: offset,
            },
            conversation,
            participants,
            active_activity_ids: runtime_state.active_activity_ids,
            active_run_ids: runtime_state.active_run_ids,
            active_context_branch_id,
            context_branches: runtime_state.context_branches,
        })
    }

    async fn conversations_inner(
        &self,
        query: ListConversations,
    ) -> Result<ConversationPage, RuntimeError> {
        let correlation_id = CorrelationId::generate();
        let conversation_id = query.conversation_id.map(|value| value.into_uuid());
        let workspace = query.workspace_id.map(|value| value.into_uuid());
        let lifecycle = query.lifecycle.map(|value| match value {
            ConversationLifecycle::Active => 1_i16,
            ConversationLifecycle::Archived => 2_i16,
        });
        let after_at = query.after.map(|value| value.updated_at.unix_millis());
        let after_id = query.after.map(|value| value.conversation_id.into_uuid());
        let rows = sqlx::query(
            "SELECT conversation.conversation_id, conversation.tenant_id,
                    conversation.workspace_id,
                    conversation.execution_owner_participant_id,
                    conversation.kind, conversation.title,
                    conversation.control_home_kind, conversation.home_epoch::text AS home_epoch,
                    conversation.lifecycle, conversation.created_by,
                    conversation.created_at_ms, conversation.updated_at_ms,
                    conversation.archived_at_ms,
                    stream.last_sequence::text AS last_sequence,
                    COALESCE(run_summary.active_run_count, 0) AS active_run_count,
                    COALESCE(run_summary.queued_run_count, 0) AS queued_run_count,
                    COALESCE(run_summary.running_run_count, 0) AS running_run_count,
                    COALESCE(run_summary.waiting_run_count, 0) AS waiting_run_count,
                    COALESCE(run_summary.cancelling_run_count, 0) AS cancelling_run_count,
                    COALESCE(run_summary.wait_approval_count, 0) AS wait_approval_count,
                    COALESCE(run_summary.wait_input_count, 0) AS wait_input_count,
                    COALESCE(run_summary.wait_device_count, 0) AS wait_device_count,
                    COALESCE(run_summary.wait_retry_backoff_count, 0)
                      AS wait_retry_backoff_count,
                    COALESCE(run_summary.wait_delegation_count, 0) AS wait_delegation_count,
                    COALESCE(run_summary.wait_external_operation_count, 0)
                      AS wait_external_operation_count,
                    COALESCE(interaction_summary.pending_interaction_count, 0)
                      AS pending_interaction_count,
                    COALESCE(interaction_summary.pending_approval_count, 0)
                      AS pending_approval_count,
                    COALESCE(interaction_summary.pending_clarification_count, 0)
                      AS pending_clarification_count,
                    COALESCE(interaction_summary.pending_confirmation_count, 0)
                      AS pending_confirmation_count,
                    COALESCE(interaction_summary.pending_selection_count, 0)
                      AS pending_selection_count,
                    COALESCE(interaction_summary.pending_credential_count, 0)
                      AS pending_credential_count,
                    COALESCE(interaction_summary.pending_oauth_count, 0)
                      AS pending_oauth_count
             FROM conversation.conversations AS conversation
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = conversation.tenant_id
              AND stream.conversation_id = conversation.conversation_id
             LEFT JOIN (
               SELECT tenant_id, conversation_id,
                      COUNT(*) AS active_run_count,
                      COUNT(*) FILTER (WHERE status = 1) AS queued_run_count,
                      COUNT(*) FILTER (WHERE status = 2) AS running_run_count,
                      COUNT(*) FILTER (WHERE status = 3) AS waiting_run_count,
                      COUNT(*) FILTER (WHERE status = 4) AS cancelling_run_count,
                      COUNT(*) FILTER (WHERE status = 3 AND wait_reason = 1)
                        AS wait_approval_count,
                      COUNT(*) FILTER (WHERE status = 3 AND wait_reason = 2)
                        AS wait_input_count,
                      COUNT(*) FILTER (WHERE status = 3 AND wait_reason = 3)
                        AS wait_device_count,
                      COUNT(*) FILTER (WHERE status = 3 AND wait_reason = 4)
                        AS wait_retry_backoff_count,
                      COUNT(*) FILTER (WHERE status = 3 AND wait_reason IN (6, 8, 9))
                        AS wait_delegation_count,
                      COUNT(*) FILTER (WHERE status = 3 AND wait_reason = 7)
                        AS wait_external_operation_count
               FROM runtime.runs
               WHERE tenant_id = $1 AND status IN (1,2,3,4)
               GROUP BY tenant_id, conversation_id
             ) AS run_summary
               ON run_summary.tenant_id = conversation.tenant_id
              AND run_summary.conversation_id = conversation.conversation_id
             LEFT JOIN (
               SELECT tenant_id, conversation_id,
                      COUNT(*) AS pending_interaction_count,
                      COUNT(*) FILTER (WHERE kind = 1) AS pending_approval_count,
                      COUNT(*) FILTER (WHERE kind = 2) AS pending_clarification_count,
                      COUNT(*) FILTER (WHERE kind = 3) AS pending_confirmation_count,
                      COUNT(*) FILTER (WHERE kind = 4) AS pending_selection_count,
                      COUNT(*) FILTER (WHERE kind = 5) AS pending_credential_count,
                      COUNT(*) FILTER (WHERE kind = 6) AS pending_oauth_count
               FROM approval.interactions
               WHERE tenant_id = $1 AND status = 1
               GROUP BY tenant_id, conversation_id
             ) AS interaction_summary
               ON interaction_summary.tenant_id = conversation.tenant_id
              AND interaction_summary.conversation_id = conversation.conversation_id
             WHERE conversation.purpose = 'standard' AND conversation.tenant_id = $1
               AND EXISTS (
                 SELECT 1 FROM conversation.participants AS viewer
                 WHERE viewer.tenant_id = conversation.tenant_id
                   AND viewer.conversation_id = conversation.conversation_id
                   AND viewer.kind = 1 AND viewer.principal_ref = $2
                   AND viewer.left_at_ms IS NULL
               )
               AND ($3::uuid IS NULL OR conversation.conversation_id = $3)
               AND ($4::uuid IS NULL OR conversation.workspace_id = $4)
               AND ($5::smallint IS NULL OR conversation.lifecycle = $5)
               AND ($6::bigint IS NULL OR conversation.updated_at_ms < $6
                    OR (conversation.updated_at_ms = $6 AND conversation.conversation_id < $7))
             ORDER BY conversation.updated_at_ms DESC, conversation.conversation_id DESC
             LIMIT $8",
        )
        .bind(query.actor.tenant_id.into_uuid())
        .bind(query.actor.principal_id.into_uuid())
        .bind(conversation_id)
        .bind(workspace)
        .bind(lifecycle)
        .bind(after_at)
        .bind(after_id)
        .bind(i64::from(query.page_size) + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?;
        let has_more = rows.len() > usize::from(query.page_size);
        let conversations = rows
            .into_iter()
            .take(usize::from(query.page_size))
            .map(|row| {
                let conversation = decode_conversation(&row, correlation_id)?;
                let latest = LedgerOffset::new(parse_u64(
                    row.try_get("last_sequence")
                        .map_err(|_| dependency(correlation_id))?,
                    correlation_id,
                )?);
                Ok(ConversationSummary {
                    conversation_id: conversation.conversation_id,
                    tenant_id: conversation.tenant_id,
                    workspace_id: conversation.workspace_id,
                    kind: conversation.kind,
                    title: conversation.title,
                    lifecycle: conversation.lifecycle,
                    latest_cursor: LedgerCursor {
                        conversation_id: conversation.conversation_id,
                        home_epoch: conversation.home_epoch,
                        after_conversation_seq: latest,
                    },
                    has_active_run: row
                        .try_get::<i64, _>("active_run_count")
                        .map_err(|_| dependency(correlation_id))?
                        > 0,
                    operational: decode_conversation_operational_summary(&row, correlation_id)?,
                    created_at: conversation.created_at,
                    updated_at: conversation.updated_at,
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        let next_cursor = has_more
            .then(|| conversations.last())
            .flatten()
            .map(|value| ConversationListCursor {
                updated_at: value.updated_at,
                conversation_id: value.conversation_id,
            });
        Ok(ConversationPage {
            conversations,
            next_cursor,
        })
    }

    async fn run_inner(&self, actor: Actor, run_id: RunId) -> Result<RunView, RuntimeError> {
        let correlation_id = CorrelationId::generate();
        let row = sqlx::query(
            "SELECT run.*, run.home_epoch::text AS home_epoch_text,
                    run.last_run_sequence::text AS last_run_sequence_text,
                    stream.last_sequence::text AS conversation_offset,
                    lease.attempt_id AS active_attempt_id,
                    lease.worker_id, lease.lease_id,
                    lease.fencing_token::text AS fencing_token,
                    lease.expires_at_ms,
                    attempt.attempt_number
             FROM runtime.runs AS run
             JOIN conversation.conversations AS public_conversation ON public_conversation.tenant_id=run.tenant_id AND public_conversation.conversation_id=run.conversation_id AND public_conversation.purpose='standard'
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = run.tenant_id
              AND stream.conversation_id = run.conversation_id
             JOIN conversation.participants AS viewer
               ON viewer.tenant_id = run.tenant_id
              AND viewer.conversation_id = run.conversation_id
              AND viewer.kind = 1 AND viewer.principal_ref = $2
              AND viewer.left_at_ms IS NULL
             LEFT JOIN runtime.leases AS lease
               ON lease.tenant_id = run.tenant_id AND lease.run_id = run.run_id
             LEFT JOIN runtime.attempts AS attempt
               ON attempt.tenant_id = run.tenant_id
              AND attempt.attempt_id = lease.attempt_id
             WHERE run.tenant_id = $1 AND run.run_id = $3
               AND run.control_home_kind = 1
               AND run.control_device_id IS NULL
               AND run.execution_home_kind = 1
               AND run.execution_device_id IS NULL",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(run_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?
        .ok_or_else(|| not_found(ErrorCode::RunNotFound, correlation_id))?;
        let mut view = decode_run_view(row, correlation_id).map_err(RuntimeError::from)?;
        view.input_message_id = sqlx::query_scalar::<_, uuid::Uuid>(
            "WITH RECURSIVE retry_lineage(run_id, parent_run_id, activity_kind, delegation_id, admitted_sequence) AS (
                 SELECT run.run_id, run.parent_run_id, run.activity_kind, run.delegation_id, run.admitted_sequence
                 FROM runtime.runs AS run
                 WHERE run.tenant_id = $1 AND run.conversation_id = $2 AND run.run_id = $3
                 UNION
                 SELECT parent.run_id, parent.parent_run_id, parent.activity_kind, parent.delegation_id, parent.admitted_sequence
                 FROM runtime.runs AS parent
                 JOIN retry_lineage AS child
                   ON parent.run_id = child.parent_run_id
                 WHERE parent.tenant_id = $1
                   AND parent.conversation_id = $2
                   AND child.activity_kind = 1
                   AND child.delegation_id IS NULL
                   AND parent.activity_kind = 1
                   AND parent.delegation_id IS NULL
             )
             SELECT message.message_id
             FROM retry_lineage AS lineage
             JOIN conversation.messages AS message
               ON message.tenant_id = $1
              AND message.conversation_id = $2
              AND message.run_id = lineage.run_id
              AND message.committed_sequence = lineage.admitted_sequence
             WHERE lineage.delegation_id IS NULL
               AND message.role = 1
               AND NOT EXISTS (
                   SELECT 1
                   FROM runtime.loop_controls AS control
                   WHERE control.tenant_id = message.tenant_id
                     AND control.message_id = message.message_id
               )
             LIMIT 1",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(view.run.context.conversation_id.into_uuid())
        .bind(run_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?
        .map(|value| id(value, correlation_id))
        .transpose()?;
        view.pending_interaction_ids = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT interaction_id
             FROM approval.interactions
             WHERE tenant_id = $1 AND run_id = $2 AND status = 1
             ORDER BY requested_at_ms, interaction_id",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(run_id.into_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?
        .into_iter()
        .map(|value| id(value, correlation_id))
        .collect::<Result<Vec<InteractionId>, _>>()?;
        Ok(view)
    }

    async fn authorize_cancel_inner(
        &self,
        actor: Actor,
        run_id: RunId,
        correlation_id: CorrelationId,
    ) -> Result<CancelRunAuthority, RuntimeError> {
        cancellation_authority::load(
            &self.pool,
            actor,
            run_id,
            correlation_id,
            cancellation_authority::Scope::Conversation,
        )
        .await
    }

    async fn authorize_run_control_inner(
        &self,
        actor: Actor,
        run_id: RunId,
        participant_id: ParticipantId,
        expected_home_epoch: HomeEpoch,
        correlation_id: CorrelationId,
    ) -> Result<RunControlAuthority, RuntimeError> {
        let row = sqlx::query(
            "SELECT run.conversation_id, run.activity_id,
                    run.home_epoch::text AS home_epoch,
                    run.last_run_sequence::text AS last_run_sequence,
                    stream.last_sequence::text AS conversation_offset,
                    viewer.participant_id
             FROM runtime.runs AS run
             JOIN conversation.conversations AS conversation
               ON conversation.tenant_id = run.tenant_id
              AND conversation.conversation_id = run.conversation_id
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = run.tenant_id
              AND stream.conversation_id = run.conversation_id
             JOIN conversation.participants AS viewer
               ON viewer.tenant_id = run.tenant_id
              AND viewer.conversation_id = run.conversation_id
              AND viewer.participant_id = $4
              AND viewer.kind = 1
              AND viewer.principal_ref = $2
              AND viewer.left_at_ms IS NULL
             WHERE run.tenant_id = $1
               AND run.run_id = $3
               AND run.status IN (1, 2, 3)
               AND run.control_home_kind = 1
               AND run.control_device_id IS NULL
               AND run.execution_home_kind = 1
               AND run.execution_device_id IS NULL
               AND conversation.lifecycle = 1
               AND conversation.control_home_kind = 1
               AND conversation.control_device_id IS NULL",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(participant_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?
        .ok_or_else(|| {
            runtime_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Run does not accept control from this participant",
            )
        })?;
        let home_epoch = HomeEpoch::new(parse_u64(
            row.try_get("home_epoch")
                .map_err(|_| dependency(correlation_id))?,
            correlation_id,
        )?)
        .map_err(|_| corrupt(correlation_id))?;
        if home_epoch != expected_home_epoch {
            return Err(runtime_error(
                ErrorCode::ConversationEpochStale,
                correlation_id,
                false,
                "Conversation ownership epoch is stale",
            ));
        }
        Ok(RunControlAuthority {
            tenant_id: actor.tenant_id,
            conversation_id: id(
                row.try_get("conversation_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?,
            home_epoch,
            current_offset: LedgerOffset::new(parse_u64(
                row.try_get("conversation_offset")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?),
            activity_id: id(
                row.try_get("activity_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?,
            run_id,
            last_run_sequence: musteragent_identity::RunSeq::new(parse_u64(
                row.try_get("last_run_sequence")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?)
            .map_err(|_| corrupt(correlation_id))?,
            requested_by_participant_id: id(
                row.try_get("participant_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?,
        })
    }

    async fn authorize_set_title_inner(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        correlation_id: CorrelationId,
    ) -> Result<SetConversationTitleAuthority, RuntimeError> {
        let row = sqlx::query(
            "SELECT conversation.lifecycle,
                    conversation.home_epoch::text AS home_epoch,
                    stream.last_sequence::text AS conversation_offset,
                    viewer.participant_id
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
             WHERE conversation.purpose = 'standard' AND conversation.tenant_id = $1
               AND conversation.conversation_id = $2",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?
        .ok_or_else(|| not_found(ErrorCode::ConversationNotFound, correlation_id))?;
        let lifecycle = match row
            .try_get::<i16, _>("lifecycle")
            .map_err(|_| dependency(correlation_id))?
        {
            1 => ConversationLifecycle::Active,
            2 => ConversationLifecycle::Archived,
            _ => return Err(corrupt(correlation_id)),
        };
        Ok(SetConversationTitleAuthority {
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
            lifecycle,
            requested_by_participant_id: id(
                row.try_get("participant_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?,
        })
    }

    async fn set_title_inner(
        &self,
        correlation_id: CorrelationId,
        commit: SetConversationTitleCommit,
    ) -> Result<musteragent_ledger::AppendReceipt, RuntimeError> {
        if commit.actor.tenant_id != commit.append.tenant_id
            || commit.append.records.len() != 1
            || !commit.append.outbox.is_empty()
        {
            return Err(runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Set Conversation Title commit is invalid",
            ));
        }
        let mut prepared = match self.ledger.begin_append(&commit.append).await {
            Ok(BeginAppend::Duplicate(receipt)) => return Ok(receipt),
            Ok(BeginAppend::Ready(prepared)) => prepared,
            Err(error) => return Err(map_ledger(error, correlation_id)),
        };
        let updated = sqlx::query(
            "UPDATE conversation.conversations
             SET title = $3, updated_at_ms = $4
             WHERE tenant_id = $1 AND conversation_id = $2
               AND home_epoch = $5::bigint AND lifecycle = 1",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .bind(commit.title.as_str())
        .bind(commit.updated_at.unix_millis())
        .bind(commit.append.expected_home_epoch.get().to_string())
        .execute(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        if updated.rows_affected() != 1 {
            return Err(runtime_error(
                ErrorCode::ConversationEpochStale,
                correlation_id,
                false,
                "Conversation title target is stale or archived",
            ));
        }
        prepared
            .commit(&commit.append)
            .await
            .map_err(|error| map_ledger(error, correlation_id))
    }

    async fn authorize_archive_inner(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        correlation_id: CorrelationId,
    ) -> Result<ArchiveConversationAuthority, RuntimeError> {
        let row = sqlx::query(
            "SELECT conversation.lifecycle,
                    conversation.home_epoch::text AS home_epoch,
                    stream.last_sequence::text AS conversation_offset,
                    viewer.participant_id
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
             WHERE conversation.purpose = 'standard' AND conversation.tenant_id = $1
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
        .ok_or_else(|| not_found(ErrorCode::ConversationNotFound, correlation_id))?;
        let lifecycle = match row
            .try_get::<i16, _>("lifecycle")
            .map_err(|_| dependency(correlation_id))?
        {
            1 => ConversationLifecycle::Active,
            2 => ConversationLifecycle::Archived,
            _ => return Err(corrupt(correlation_id)),
        };
        let run_rows = sqlx::query(
            "SELECT run.run_id, run.activity_id,
                    run.last_run_sequence::text AS last_run_sequence,
                    run.status, run.wait_reason, run.wait_error_code, run.wait_interaction_id,
                    CASE
                        WHEN EXISTS(SELECT 1 FROM tool.tool_invocations t WHERE t.tenant_id=run.tenant_id AND t.run_id=run.run_id AND t.invocation_id=run.wait_invocation_id AND t.status=6) THEN NULL
                        WHEN run.wait_interaction_id IS NULL
                            THEN run.wait_invocation_id
                        WHEN pending_interaction.requested_by_invocation_id
                                = run.wait_invocation_id
                             AND pending_tool.invocation_id IS NOT NULL
                            THEN run.wait_invocation_id
                        ELSE NULL
                    END AS pending_tool_invocation_id,
                    (SELECT t.invocation_id FROM tool.tool_invocations t WHERE t.tenant_id=run.tenant_id AND t.run_id=run.run_id AND t.invocation_id=run.wait_invocation_id AND t.status=6) AS unresolved_tool_invocation_id,
                    EXISTS (
                        SELECT 1 FROM runtime.leases
                        WHERE leases.tenant_id = run.tenant_id
                          AND leases.run_id = run.run_id
                    ) AS has_lease,
                    (
                        SELECT attempts.attempt_id
                        FROM runtime.attempts
                        WHERE attempts.tenant_id = run.tenant_id
                          AND attempts.run_id = run.run_id
                        ORDER BY attempts.attempt_number DESC
                        LIMIT 1
                    ) AS last_attempt_id
             FROM runtime.runs AS run
             LEFT JOIN approval.interactions AS pending_interaction
               ON pending_interaction.tenant_id = run.tenant_id
              AND pending_interaction.conversation_id = run.conversation_id
              AND pending_interaction.run_id = run.run_id
              AND pending_interaction.interaction_id = run.wait_interaction_id
              AND pending_interaction.status = 1
             LEFT JOIN tool.tool_invocations AS pending_tool
               ON pending_tool.tenant_id = run.tenant_id
              AND pending_tool.conversation_id = run.conversation_id
              AND pending_tool.run_id = run.run_id
              AND pending_tool.invocation_id = run.wait_invocation_id
              AND pending_tool.interaction_id = run.wait_interaction_id
              AND pending_tool.status IN (2, 3, 6)
             WHERE run.tenant_id = $1
               AND run.conversation_id = $2
               AND run.status IN (1, 2, 3, 4)
             ORDER BY run.admitted_at_ms, run.run_id",
        )
        .bind(actor.tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| dependency(correlation_id))?;
        let active_runs = run_rows
            .into_iter()
            .map(|run| decode_archive_target(run, correlation_id))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ArchiveConversationAuthority {
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
            lifecycle,
            requested_by_participant_id: id(
                row.try_get("participant_id")
                    .map_err(|_| dependency(correlation_id))?,
                correlation_id,
            )?,
            active_runs,
        })
    }

    async fn archive_inner(
        &self,
        correlation_id: CorrelationId,
        commit: ArchiveConversationCommit,
    ) -> Result<musteragent_ledger::AppendReceipt, RuntimeError> {
        validate_archive(&commit).map_err(|code| {
            runtime_error(
                code,
                correlation_id,
                false,
                "Archive Conversation commit is invalid",
            )
        })?;
        let mut prepared = match self.ledger.begin_append(&commit.append).await {
            Ok(BeginAppend::Duplicate(receipt)) => return Ok(receipt),
            Ok(BeginAppend::Ready(prepared)) => prepared,
            Err(error) => return Err(map_ledger(error, correlation_id)),
        };
        let lifecycle: Option<i16> = sqlx::query_scalar(
            "SELECT lifecycle
             FROM conversation.conversations
             WHERE tenant_id = $1 AND conversation_id = $2
               AND home_epoch = $3::bigint
             FOR UPDATE",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .bind(commit.append.expected_home_epoch.get().to_string())
        .fetch_optional(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        match lifecycle {
            Some(1) => {}
            Some(2) => {
                return Err(runtime_error(
                    ErrorCode::ConversationArchived,
                    correlation_id,
                    false,
                    "Conversation is already archived",
                ));
            }
            _ => {
                return Err(runtime_error(
                    ErrorCode::ConversationEpochStale,
                    correlation_id,
                    false,
                    "Conversation ownership epoch is stale",
                ));
            }
        }
        let persisted_runs = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT run_id
             FROM runtime.runs
             WHERE tenant_id = $1 AND conversation_id = $2
               AND status IN (1, 2, 3, 4)
             ORDER BY run_id
             FOR UPDATE",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .fetch_all(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        let requested_runs = commit
            .targets
            .iter()
            .map(|target| target.run_id.into_uuid())
            .collect::<BTreeSet<_>>();
        if persisted_runs.into_iter().collect::<BTreeSet<_>>() != requested_runs {
            return Err(runtime_error(
                ErrorCode::RunTransitionInvalid,
                correlation_id,
                false,
                "Active Runs changed while archiving the Conversation",
            ));
        }
        let now = commit.archived_at;
        for target in &commit.targets {
            apply_cancel_mutation(
                prepared.transaction_mut(),
                &CancelMutation {
                    tenant_id: commit.append.tenant_id,
                    conversation_id: commit.append.conversation_id,
                    home_epoch: commit.append.expected_home_epoch,
                    run_id: target.run_id,
                    activity_id: target.activity_id,
                    expected_last_run_sequence: target.last_run_sequence,
                    mode: target.mode,
                    pending_interaction_id: target.pending_interaction_id,
                    pending_tool_invocation_id: target.pending_tool_invocation_id,
                    unresolved_tool_invocation_id: target.unresolved_tool_invocation_id,
                    record_count: archive_target_record_count(target),
                },
                now,
            )
            .await
            .map_err(|error| map_execution(error, correlation_id))?;
        }
        let updated = sqlx::query(
            "UPDATE conversation.conversations
             SET lifecycle = 2, archived_at_ms = $3, updated_at_ms = $3
             WHERE tenant_id = $1 AND conversation_id = $2 AND lifecycle = 1",
        )
        .bind(commit.actor.tenant_id.into_uuid())
        .bind(commit.append.conversation_id.into_uuid())
        .bind(now.unix_millis())
        .execute(&mut *prepared.transaction_mut())
        .await
        .map_err(|_| dependency(correlation_id))?;
        if updated.rows_affected() != 1 {
            return Err(runtime_error(
                ErrorCode::ConversationArchived,
                correlation_id,
                false,
                "Conversation is already archived",
            ));
        }
        prepared
            .commit(&commit.append)
            .await
            .map_err(|error| map_ledger(error, correlation_id))
    }
}

impl RuntimeApplicationStore for PostgresRuntimeApplicationStore {
    fn load_command_replay(
        &self,
        query: musteragent_runtime_session::CommandReplayQuery,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<Option<musteragent_ledger::AppendReceipt>, RuntimeError>>
    {
        Box::pin(async move { self.load_command_replay_inner(query, correlation_id).await })
    }

    fn recover_conversation_creation(
        &self,
        query: ConversationCreationRecoveryQuery,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, RuntimeError>> {
        Box::pin(async move { creation_recovery::recover(&self.pool, query, correlation_id).await })
    }

    fn load_admission_replay(
        &self,
        query: musteragent_runtime_session::CommandReplayQuery,
        runtime_snapshot_id: RuntimeSnapshotId,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<Option<AdmissionReplayReceipt>, RuntimeError>> {
        Box::pin(async move {
            let receipt = self
                .load_command_replay_inner(query.clone(), correlation_id)
                .await?;
            let Some(ledger) = receipt else {
                return Ok(None);
            };
            let created_at_ms: i64 = sqlx::query_scalar(
                "SELECT created_at_ms FROM capability.runtime_snapshots
                 WHERE tenant_id = $1 AND conversation_id = $2
                   AND runtime_snapshot_id = $3",
            )
            .bind(query.tenant_id.into_uuid())
            .bind(query.conversation_id.into_uuid())
            .bind(runtime_snapshot_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| dependency(correlation_id))?
            .ok_or_else(|| corrupt(correlation_id))?;
            Ok(Some(AdmissionReplayReceipt {
                ledger,
                prepared_at: UtcTimestamp::from_unix_millis(created_at_ms),
            }))
        })
    }

    fn load_admission_denial(
        &self,
        query: musteragent_runtime_session::AdmissionDenialQuery,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<
        '_,
        Result<Option<musteragent_runtime_session::AdmissionDenialReceipt>, RuntimeError>,
    > {
        Box::pin(async move {
            self.load_admission_denial_inner(query, correlation_id)
                .await
        })
    }

    fn record_admission_denial(
        &self,
        commit: musteragent_runtime_session::AdmissionDenialCommit,
    ) -> ApplicationFuture<
        '_,
        Result<musteragent_runtime_session::AdmissionDenialReceipt, RuntimeError>,
    > {
        Box::pin(async move { self.record_admission_denial_inner(commit).await })
    }

    fn load_assistant_identity(
        &self,
        agent_key: musteragent_identity::AgentKey,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<AgentAdmissionIdentity, RuntimeError>> {
        Box::pin(async move {
            self.catalog
                .resolve(&agent_key)
                .await
                .map(|(definition, version)| AgentAdmissionIdentity {
                    definition,
                    version,
                })
                .map_err(|_| {
                    runtime_error(
                        ErrorCode::AgentNotAvailable,
                        correlation_id,
                        false,
                        "The requested Agent is not available",
                    )
                })
        })
    }

    fn create_conversation(
        &self,
        command_id: CommandId,
        correlation_id: CorrelationId,
        commit: CreateConversationCommit,
    ) -> ApplicationFuture<'_, Result<CommandAck, RuntimeError>> {
        Box::pin(async move { self.create_inner(command_id, correlation_id, commit).await })
    }

    fn authorize_admission(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        expected_home_epoch: HomeEpoch,
        correlation_id: CorrelationId,
        _audit_context: AdmissionAuditContext,
    ) -> ApplicationFuture<'_, Result<AdmissionAuthority, RuntimeError>> {
        Box::pin(async move {
            self.authorize_inner(
                actor,
                conversation_id,
                participant_id,
                expected_home_epoch,
                correlation_id,
            )
            .await
        })
    }

    fn authorize_admission_for_home(
        &self,
        request: musteragent_runtime_session::AdmissionAuthorizationRequest,
    ) -> ApplicationFuture<'_, Result<AdmissionAuthority, RuntimeError>> {
        Box::pin(async move {
            if request.expected_control_home != RuntimeHome::Cloud {
                return Err(runtime_error(
                    ErrorCode::PolicyDenied,
                    request.correlation_id,
                    false,
                    "Runtime store does not support the configured Conversation home",
                ));
            }
            let authority = self
                .authorize_inner(
                    request.actor,
                    request.conversation_id,
                    request.participant_id,
                    request.expected_home_epoch,
                    request.correlation_id,
                )
                .await?;
            self.enforce_foreground_priority(
                request.actor.tenant_id,
                request.conversation_id,
                request.activity_kind,
                request.correlation_id,
            )
            .await?;
            Ok(authority)
        })
    }

    fn admit(
        &self,
        _command_id: CommandId,
        correlation_id: CorrelationId,
        batch: AdmissionBatch,
    ) -> ApplicationFuture<'_, Result<AdmissionReceipt, RuntimeError>> {
        Box::pin(async move {
            self.admission
                .admit(batch)
                .await
                .map_err(|error| map_admission(error, correlation_id))
        })
    }

    fn admit_content(
        &self,
        _command_id: CommandId,
        correlation_id: CorrelationId,
        surface: musteragent_scheduler::AdmissionSurface,
        commit: AdmissionCommit,
    ) -> ApplicationFuture<'_, Result<AdmissionReceipt, RuntimeError>> {
        Box::pin(async move {
            let receipt = if surface == musteragent_scheduler::AdmissionSurface::Api {
                self.admission.admit_api_content(commit).await
            } else {
                self.admission.admit_content(commit).await
            };
            receipt.map_err(|error| map_admission(error, correlation_id))
        })
    }

    fn admit_goal_start(
        &self,
        command_id: CommandId,
        correlation_id: CorrelationId,
        surface: musteragent_scheduler::AdmissionSurface,
        commit: musteragent_runtime_session::GoalStartAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<AdmissionReceipt, RuntimeError>> {
        Box::pin(async move {
            if command_id != commit.admission.content_stage.command_id {
                return Err(runtime_error(
                    ErrorCode::ContentIntegrityFailed,
                    correlation_id,
                    false,
                    "Goal start content stage belongs to another command",
                ));
            }
            self.admission
                .admit_goal_start(commit, surface)
                .await
                .map_err(|error| map_admission(error, correlation_id))
        })
    }

    fn admit_retry(
        &self,
        correlation_id: CorrelationId,
        batch: RetryAdmissionBatch,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, RuntimeError>> {
        Box::pin(async move {
            self.admission
                .admit_retry(batch)
                .await
                .map_err(|error| map_admission(error, correlation_id))
        })
    }

    fn admit_coordination_content(
        &self,
        correlation_id: CorrelationId,
        commit: musteragent_runtime_session::CoordinationAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, RuntimeError>> {
        Box::pin(async move {
            commit.validate().map_err(|code| {
                runtime_error(
                    code,
                    correlation_id,
                    false,
                    "Cross-Conversation admission authority does not match the durable batch",
                )
            })?;
            self.admission
                .admit_coordination_content(commit)
                .await
                .map_err(|error| map_admission(error, correlation_id))
        })
    }

    fn admit_standing_goal(
        &self,
        correlation_id: CorrelationId,
        commit: StandingGoalAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::StandingGoalCommitOutcome, RuntimeError>>
    {
        Box::pin(async move {
            let StandingGoalAdmissionCommit {
                authority,
                command_id,
                goal_id,
                pursuit_id,
                batch,
            } = commit;
            if command_id.into_uuid() == uuid::Uuid::nil()
                || goal_id != batch.goal_id
                || pursuit_id != batch.pursuit_id
                || authority.tenant_id != batch.append.tenant_id
                || authority.conversation_id != batch.append.conversation_id
                || authority.home_epoch != batch.append.expected_home_epoch
                || authority.current_offset != batch.append.expected_offset
                || authority.requested_by_participant_id
                    != batch.activity.requested_by_participant_id
                || authority.executor_participant_id != batch.run.context.executor_participant_id
            {
                return Err(runtime_error(
                    ErrorCode::ContentIntegrityFailed,
                    correlation_id,
                    false,
                    "Standing Goal admission authority does not match the durable batch",
                ));
            }
            self.admission
                .admit_standing_goal(batch)
                .await
                .map_err(|error| map_admission(error, correlation_id))
        })
    }

    fn get_conversation(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
    ) -> ApplicationFuture<'_, Result<ConversationView, RuntimeError>> {
        Box::pin(async move { self.conversation_inner(actor, conversation_id).await })
    }

    fn list_conversations(
        &self,
        query: ListConversations,
    ) -> ApplicationFuture<'_, Result<ConversationPage, RuntimeError>> {
        Box::pin(async move { self.conversations_inner(query).await })
    }

    fn get_run(
        &self,
        actor: Actor,
        run_id: RunId,
    ) -> ApplicationFuture<'_, Result<RunView, RuntimeError>> {
        Box::pin(async move { self.run_inner(actor, run_id).await })
    }

    fn load_run_runtime_snapshot(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        run_id: RunId,
        runtime_snapshot_id: RuntimeSnapshotId,
        expected_content_hash: Option<Sha256>,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<
        '_,
        Result<musteragent_runtime_snapshot::RuntimeSnapshotContent, RuntimeError>,
    > {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT encoded_snapshot, content_hash
                 FROM capability.runtime_snapshots
                 WHERE tenant_id = $1 AND conversation_id = $2
                   AND run_id = $3 AND runtime_snapshot_id = $4",
            )
            .bind(tenant_id.into_uuid())
            .bind(conversation_id.into_uuid())
            .bind(run_id.into_uuid())
            .bind(runtime_snapshot_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| dependency(correlation_id))?
            .ok_or_else(|| corrupt(correlation_id))?;
            let encoded_snapshot: Vec<u8> = row
                .try_get("encoded_snapshot")
                .map_err(|_| dependency(correlation_id))?;
            let persisted_hash = Sha256::from_bytes(
                row.try_get::<Vec<u8>, _>("content_hash")
                    .map_err(|_| dependency(correlation_id))?,
            )
            .map_err(|_| corrupt(correlation_id))?;
            if persisted_hash != Sha256::digest(&encoded_snapshot)
                || expected_content_hash.is_some_and(|expected| expected != persisted_hash)
            {
                return Err(corrupt(correlation_id));
            }
            let snapshot = self
                .snapshots
                .decode_runtime_snapshot(&encoded_snapshot)
                .map_err(|_| corrupt(correlation_id))?;
            snapshot
                .validate_sealed()
                .map_err(|_| corrupt(correlation_id))?;
            Ok(snapshot)
        })
    }

    fn load_run_discovery(
        &self,
        query: RunDiscoveryQuery,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<
        '_,
        Result<Option<musteragent_runtime_session::PriorRunDiscovery>, RuntimeError>,
    > {
        Box::pin(async move {
            self.load_run_discovery(
                query.tenant_id,
                query.conversation_id,
                query.home_epoch,
                ReachableContextPoint {
                    lineage_head_branch_id: query.lineage_head_branch_id,
                    through_offset: query.through_offset,
                },
                query.source_run_id,
                correlation_id,
            )
            .await
        })
    }

    fn authorize_work_item_cancel_run(
        &self,
        fence: musteragent_runtime_session::WorkItemRunControlFence,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<CancelRunAuthority, RuntimeError>> {
        Box::pin(self.authorize_work_item_cancel_inner(fence, correlation_id))
    }
    fn cancel_work_item_run(
        &self,
        correlation_id: CorrelationId,
        commit: musteragent_runtime_session::WorkItemCancelRunCommit,
    ) -> ApplicationFuture<'_, Result<CancelRunReceipt, RuntimeError>> {
        Box::pin(self.cancel_work_item_inner(correlation_id, commit))
    }
    fn authorize_dialogue_cancel_run(
        &self,
        fence: musteragent_runtime_session::DialogueRunControlFence,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<CancelRunAuthority, RuntimeError>> {
        Box::pin(self.authorize_dialogue_cancel_inner(fence, correlation))
    }
    fn cancel_dialogue_run(
        &self,
        correlation: CorrelationId,
        commit: musteragent_runtime_session::DialogueCancelRunCommit,
    ) -> ApplicationFuture<'_, Result<CancelRunReceipt, RuntimeError>> {
        Box::pin(self.cancel_dialogue_inner(correlation, commit))
    }
    fn authorize_cancel_run(
        &self,
        actor: Actor,
        run_id: RunId,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<CancelRunAuthority, RuntimeError>> {
        Box::pin(async move {
            self.authorize_cancel_inner(actor, run_id, correlation_id)
                .await
        })
    }

    fn cancel_run(
        &self,
        correlation_id: CorrelationId,
        batch: CancelRunBatch,
    ) -> ApplicationFuture<'_, Result<CancelRunReceipt, RuntimeError>> {
        Box::pin(async move {
            self.ledger
                .cancel_run(batch)
                .await
                .map_err(|error| map_execution(error, correlation_id))
        })
    }

    fn authorize_run_control(
        &self,
        actor: Actor,
        run_id: RunId,
        participant_id: ParticipantId,
        expected_home_epoch: HomeEpoch,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<RunControlAuthority, RuntimeError>> {
        Box::pin(async move {
            self.authorize_run_control_inner(
                actor,
                run_id,
                participant_id,
                expected_home_epoch,
                correlation_id,
            )
            .await
        })
    }

    fn resume_run(
        &self,
        correlation_id: CorrelationId,
        commit: musteragent_runtime_session::RunResumeCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, RuntimeError>> {
        Box::pin(async move { self.resume_run_inner(correlation_id, commit).await })
    }

    fn enqueue_run_control(
        &self,
        correlation_id: CorrelationId,
        batch: RunControlBatch,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, RuntimeError>> {
        Box::pin(async move {
            self.ledger
                .enqueue_run_control(batch)
                .await
                .map_err(|error| map_run_control(error, correlation_id))
        })
    }

    fn enqueue_run_control_content(
        &self,
        correlation_id: CorrelationId,
        commit: RunControlCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, RuntimeError>> {
        Box::pin(async move {
            let content = self.content.as_ref().ok_or_else(|| {
                runtime_error(
                    ErrorCode::DependencyUnavailable,
                    correlation_id,
                    true,
                    "Admission content publisher is unavailable",
                )
            })?;
            self.ledger
                .enqueue_run_control_content(commit.batch, content, commit.content_stage)
                .await
                .map_err(|error| map_run_control(error, correlation_id))
        })
    }

    fn authorize_retract_message(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        message_id: MessageId,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<RetractMessageAuthority, RuntimeError>> {
        Box::pin(async move {
            self.authorize_retract_inner(actor, conversation_id, message_id, correlation_id)
                .await
        })
    }

    fn retract_message(
        &self,
        correlation_id: CorrelationId,
        commit: RetractMessageCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, RuntimeError>> {
        Box::pin(async move { self.retract_inner(correlation_id, commit).await })
    }

    fn authorize_fork_context(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        parent_branch_id: Option<ContextBranchId>,
        fork_from_record_id: LedgerRecordId,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<ForkContextAuthority, RuntimeError>> {
        Box::pin(async move {
            self.authorize_fork_context_inner(
                actor,
                conversation_id,
                parent_branch_id,
                fork_from_record_id,
                correlation_id,
            )
            .await
        })
    }

    fn fork_context(
        &self,
        correlation_id: CorrelationId,
        commit: ForkContextCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, RuntimeError>> {
        Box::pin(async move { self.fork_context_inner(correlation_id, commit).await })
    }

    fn authorize_activate_context_branch(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        branch_id: ContextBranchId,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<ContextBranchAuthority, RuntimeError>> {
        Box::pin(async move {
            self.authorize_activate_context_branch_inner(
                actor,
                conversation_id,
                branch_id,
                correlation_id,
            )
            .await
        })
    }

    fn activate_context_branch(
        &self,
        correlation_id: CorrelationId,
        commit: ActivateContextBranchCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, RuntimeError>> {
        Box::pin(async move {
            self.activate_context_branch_inner(correlation_id, commit)
                .await
        })
    }

    fn authorize_edit_and_resubmit(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        target_message_id: MessageId,
        expected_home_epoch: HomeEpoch,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<EditAndResubmitAuthority, RuntimeError>> {
        Box::pin(async move {
            self.authorize_edit_and_resubmit_inner(
                actor,
                conversation_id,
                participant_id,
                target_message_id,
                expected_home_epoch,
                correlation_id,
            )
            .await
        })
    }

    fn edit_and_resubmit(
        &self,
        correlation_id: CorrelationId,
        commit: EditAndResubmitCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AdmissionReceipt, RuntimeError>> {
        Box::pin(async move { self.edit_and_resubmit_inner(correlation_id, commit).await })
    }

    fn edit_and_resubmit_content(
        &self,
        correlation_id: CorrelationId,
        commit: EditAndResubmitAdmissionCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AdmissionReceipt, RuntimeError>> {
        Box::pin(async move {
            self.edit_and_resubmit_content_inner(correlation_id, commit)
                .await
        })
    }

    fn authorize_archive_conversation(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<ArchiveConversationAuthority, RuntimeError>> {
        Box::pin(async move {
            self.authorize_archive_inner(actor, conversation_id, correlation_id)
                .await
        })
    }

    fn archive_conversation(
        &self,
        correlation_id: CorrelationId,
        commit: ArchiveConversationCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, RuntimeError>> {
        Box::pin(async move { self.archive_inner(correlation_id, commit).await })
    }

    fn authorize_set_conversation_title(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        correlation_id: CorrelationId,
    ) -> ApplicationFuture<'_, Result<SetConversationTitleAuthority, RuntimeError>> {
        Box::pin(async move {
            self.authorize_set_title_inner(actor, conversation_id, correlation_id)
                .await
        })
    }

    fn set_conversation_title(
        &self,
        correlation_id: CorrelationId,
        commit: SetConversationTitleCommit,
    ) -> ApplicationFuture<'_, Result<musteragent_ledger::AppendReceipt, RuntimeError>> {
        Box::pin(async move { self.set_title_inner(correlation_id, commit).await })
    }
}

fn validate_execution_owner(
    conversation: &Conversation,
    participants: &[Participant],
    correlation_id: CorrelationId,
) -> Result<(), RuntimeError> {
    let owner_is_valid = participants.iter().any(|participant| {
        participant.participant_id == conversation.execution_owner_participant_id
            && participant.kind == ParticipantKind::Agent
            && participant.agent_ref.is_some()
            && participant.left_at.is_none()
    });
    if owner_is_valid {
        Ok(())
    } else {
        Err(corrupt(correlation_id))
    }
}
