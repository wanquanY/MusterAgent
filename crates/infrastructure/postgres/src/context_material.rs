mod agent_instructions;
mod assembly;
mod baseline;
mod coordination;
mod delegation;
mod dialogue;
mod dialogue_capacity;
mod dialogue_history;
mod dialogue_material;
mod dialogue_preview;
mod error;
mod instructions;
mod interrupted_exchange;
mod material_codec;
mod measured_usage;
mod model_action_rejection;
mod preparation;
mod provenance;
mod public_delivery_recovery;
mod public_delivery_refs;
mod public_delivery_repair;
mod run_source;
mod source_port;
mod source_selection;
mod status_policy;
mod terminal;
mod tool_codec;
mod tool_exchanges;
mod visible_attachments;
use coordination::CoordinationInputSource;
pub use error::PostgresContextMaterialError;
use material_codec::*;
use model_action_rejection::load_latest_model_action_rejection;
use provenance::{latest_completion_for_run, load_assistant_origins, message_assistant_origin};
use run_source::RunSource;
use status_policy::*;
use terminal::{ToolTerminalSource, ToolTerminalSourceKind, terminal_result_matches};
pub(crate) use tool_codec::decode_tool_message;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use musteragent_activity::{ActivityEvent, ActivityKind};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_context::{
    AssistantOrigin, AttachmentMaterializationError, ConsumedStimulusCut, ContextEntry,
    ContextMaterial, ContextMaterialSource, ContextPlanError, ContextPlanInput,
    ContextPresentationOrder, ContextToolPolicy, ContextUsageAnchor,
    ConversationVisibilityBoundary, DialogueContextWindow, MaterializedContent,
    MaterializedContextBaseline, MaterializedExecutionPlan, MaterializedInstinct,
    MaterializedMemory, MaterializedModelActionRejection, MaterializedSkill,
    RuntimeSnapshotDecoder, materialize_message_attachment_from_batch, message_attachment_text_ref,
};
use musteragent_conversation::{ConversationEvent, MessageRole};
use musteragent_discovery::{DiscoveryEvent, ExposureState, SkillDisclosed, ToolExposureAdded};
use musteragent_event_envelope::{ExecutionScope, LedgerRecordKind, RuntimeHome};
use musteragent_identity::{
    ActivityId, ContentId, ContextBranchId, ContextSnapshotId, ConversationId, ConversationSeq,
    DelegationId, DeviceId, GoalId, GoalPursuitId, InvocationId, LedgerRecordId, MemoryId,
    MemoryVersionId, MessageId, ParticipantId, RunId, RuntimeSnapshotId, TenantId, ToolCallId,
    VersionedKey, WorkspaceId,
};
use musteragent_ledger::{DomainEvent, LedgerPayload};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::RunContext;
use musteragent_runtime_content::{ContentBatch, ContentStore, verify_content};
use musteragent_runtime_model::{
    CanonicalContentPart, CanonicalMessage, CanonicalRole, CanonicalToolCall, JsonValue,
    ModelEvent, ModelRouteRef, ModelText, ReasoningReplay,
};
use musteragent_runtime_snapshot::{RuntimeSnapshotContent, SealedSnapshotCache};
use musteragent_tool::{ToolEvent, ToolKey, WireName};
use sqlx::{PgPool, Row};

#[derive(Clone)]
pub struct PostgresContextMaterialSource {
    preparation: Option<Arc<preparation::SnapshotPreparation>>,
    application_catalogs: Option<
        Arc<
            dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
        >,
    >,
    pool: PgPool,
    database: crate::PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
    snapshots: Arc<dyn RuntimeSnapshotDecoder>,
    ledger_decoder: Arc<dyn LedgerPayloadDecoder>,
}

impl PostgresContextMaterialSource {
    pub fn with_application_catalogs(
        mut self,
        source: Option<Arc<dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource>>,
    ) -> Self {
        self.application_catalogs = source;
        self
    }

    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        snapshots: Arc<dyn RuntimeSnapshotDecoder>,
        ledger_decoder: Arc<dyn LedgerPayloadDecoder>,
    ) -> Self {
        let database = database.into();
        Self {
            preparation: None,
            application_catalogs: None,
            pool: database.pool().clone(),
            database,
            content,
            snapshots,
            ledger_decoder,
        }
    }

    pub async fn load_inner(
        &self,
        input: &ContextPlanInput,
    ) -> Result<ContextMaterial, PostgresContextMaterialError> {
        let recorded = Arc::new(musteragent_runtime_content::RecordingContentStore::new(
            self.content.clone(),
        ));
        let mut source = self.clone();
        source.content = recorded.clone();
        let mut material = source.load_material_inner(input).await?;
        material.source_content_refs = recorded.references()?;
        Ok(material)
    }

    async fn load_material_inner(
        &self,
        input: &ContextPlanInput,
    ) -> Result<ContextMaterial, PostgresContextMaterialError> {
        let (source, dialogue_window) = self.load_run_source(input).await?;
        // These readers retain independent fresh authority checks. Current
        // Dialogue instructions/approvals need not wait for immutable content
        // and Ledger material; commit and dispatch retain their original fences.
        let (mut material, dialogue) = tokio::try_join!(
            self.load_material_for_source(input, source, dialogue_window),
            self.current_dialogue_context(input),
        )?;
        material.agent_instructions = dialogue.instructions;
        material.dialogue_approval_context = dialogue.approvals;
        material.dialogue_execution_context = dialogue.executions;
        Ok(material)
    }

    async fn load_model_action_rejection(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
        cutoff: ConversationSeq,
    ) -> Result<Option<MaterializedModelActionRejection>, PostgresContextMaterialError> {
        if let Some(value) = public_delivery_repair::load(self, tenant_id, run_id, cutoff).await? {
            return Ok(value);
        }
        load_latest_model_action_rejection(&self.pool, tenant_id, run_id).await
    }

    /// The fenced Run row names the sealed Snapshot digest. Decoded content is
    /// shared process-wide by `(id, digest)`; bytes are read, hashed, decoded
    /// and sealed-validated only on the first step that needs them.
    async fn sealed_runtime_snapshot(
        &self,
        tenant_id: TenantId,
        runtime_snapshot_id: RuntimeSnapshotId,
        content_hash: Sha256,
    ) -> Result<Arc<RuntimeSnapshotContent>, PostgresContextMaterialError> {
        let cache = SealedSnapshotCache::global();
        if let Some(snapshot) = cache.get(runtime_snapshot_id, content_hash) {
            return Ok(snapshot);
        }
        let encoded_snapshot: Vec<u8> = sqlx::query_scalar(
            "SELECT encoded_snapshot
             FROM capability.runtime_snapshots
             WHERE runtime_snapshot_id = $1
               AND tenant_id = $2",
        )
        .bind(runtime_snapshot_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PostgresContextMaterialError::CorruptState)?;
        if Sha256::digest(&encoded_snapshot) != content_hash {
            return Err(PostgresContextMaterialError::CorruptState);
        }
        let snapshot = self
            .snapshots
            .decode_runtime_snapshot(&encoded_snapshot)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        cache
            .admit(runtime_snapshot_id, content_hash, snapshot)
            .map_err(|_| PostgresContextMaterialError::CorruptState)
    }

    /// Read the admitted run identity under the current worker lease fence.
    pub async fn fenced_run_context(
        &self,
        attempt: &musteragent_run::AttemptContext,
    ) -> Result<RunContext, PostgresContextMaterialError> {
        self.load_fenced_header(attempt)
            .await
            .map(|header| header.0)
    }

    async fn load_fenced_header(
        &self,
        attempt: &musteragent_run::AttemptContext,
    ) -> Result<(RunContext, Sha256, ConversationSeq, u64), PostgresContextMaterialError> {
        let row = sqlx::query(
            "SELECT run.tenant_id, run.conversation_id, run.activity_id,
                    run.activity_kind, run.executor_participant_id,
                    run.requested_by_participant_id, run.parent_run_id,
                    run.delegation_id, run.execution_scope_kind,
                    run.workspace_id, run.device_id, run.control_home_kind,
                    run.control_device_id, run.execution_home_kind,
                    run.execution_device_id,
                    run.home_epoch::text AS home_epoch,
                    run.runtime_snapshot_id, snapshot.content_hash,
                    run.admitted_sequence::text AS admitted_sequence,
                    stream.last_sequence::text AS ledger_head
             FROM runtime.runs AS run
             JOIN runtime.attempts AS attempt
               ON attempt.tenant_id = run.tenant_id
              AND attempt.run_id = run.run_id
             JOIN runtime.leases AS lease
               ON lease.tenant_id = run.tenant_id
              AND lease.run_id = run.run_id
              AND lease.attempt_id = attempt.attempt_id
             JOIN capability.runtime_snapshots AS snapshot
               ON snapshot.runtime_snapshot_id = run.runtime_snapshot_id
              AND snapshot.tenant_id = run.tenant_id
              AND snapshot.conversation_id = run.conversation_id
              AND snapshot.run_id = run.run_id
             JOIN event_store.ledger_streams AS stream
               ON stream.tenant_id = run.tenant_id
              AND stream.conversation_id = run.conversation_id
             WHERE run.run_id = $1
               AND run.status IN (2, 4)
               AND attempt.attempt_id = $2
               AND attempt.attempt_number = $3
               AND attempt.status = 1
               AND lease.worker_id = $4
               AND lease.lease_id = $5
               AND lease.fencing_token = $6::bigint
               AND lease.expires_at_ms >
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
        )
        .bind(attempt.run_id.into_uuid())
        .bind(attempt.attempt_id.into_uuid())
        .bind(i32::try_from(attempt.attempt_number.get())?)
        .bind(attempt.worker_id.as_str())
        .bind(attempt.lease_id.into_uuid())
        .bind(attempt.fencing_token.get().to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PostgresContextMaterialError::StaleFence)?;
        let content_hash = Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        let tenant_id = TenantId::from_uuid(row.try_get("tenant_id")?)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        let execution_scope = execution_scope(
            row.try_get("execution_scope_kind")?,
            tenant_id,
            row.try_get("workspace_id")?,
            row.try_get("device_id")?,
        )?;
        let activity_kind = activity_kind(row.try_get("activity_kind")?)?;
        let parent_run_id = optional_id(row.try_get("parent_run_id")?)?;
        let run_context = RunContext {
            tenant_id,
            conversation_id: id(row.try_get("conversation_id")?)?,
            activity_id: id(row.try_get("activity_id")?)?,
            activity_kind,
            run_id: attempt.run_id,
            executor_participant_id: id(row.try_get("executor_participant_id")?)?,
            requested_by_participant_id: id(row.try_get("requested_by_participant_id")?)?,
            parent_run_id,
            delegation_id: optional_id(row.try_get("delegation_id")?)?,
            execution_scope,
            control_home: runtime_home(
                row.try_get("control_home_kind")?,
                row.try_get("control_device_id")?,
            )?,
            execution_home: runtime_home(
                row.try_get("execution_home_kind")?,
                row.try_get("execution_device_id")?,
            )?,
            home_epoch: musteragent_identity::HomeEpoch::new(parse_u64(
                row.try_get("home_epoch")?,
            )?)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?,
            runtime_snapshot_id: id(row.try_get("runtime_snapshot_id")?)?,
        };
        let admitted_sequence = parse_u64(row.try_get("admitted_sequence")?)?;
        let ledger_head = parse_u64(row.try_get("ledger_head")?)?;
        let admitted_sequence = ConversationSeq::new(admitted_sequence)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        Ok((run_context, content_hash, admitted_sequence, ledger_head))
    }

    async fn load_run_source(
        &self,
        input: &ContextPlanInput,
    ) -> Result<(RunSource, Option<DialogueContextWindow>), PostgresContextMaterialError> {
        if let Some(preparation) = &self.preparation {
            preparation.require(input)?;
        }
        let (run_context, content_hash, admitted_sequence, ledger_head) =
            self.load_fenced_header(&input.attempt).await?;
        let tenant_id = run_context.tenant_id;
        let activity_kind = run_context.activity_kind;
        let parent_run_id = run_context.parent_run_id;
        let ledger_cutoff = ledger_head;
        let sealed = self
            .sealed_runtime_snapshot(tenant_id, run_context.runtime_snapshot_id, content_hash)
            .await?;
        let snapshot = match &self.preparation {
            Some(preparation) => preparation.snapshot(content_hash, sealed)?,
            None => sealed,
        };
        let snapshot = RuntimeSnapshotContent::clone(&snapshot);
        let mut source = RunSource {
            observed_ledger_head: ledger_head,
            superseding_control_id: None,
            run_context,
            snapshot,
            visibility: ConversationVisibilityBoundary {
                target_run_id: input.attempt.run_id,
                admitted_sequence,
                include_predecessor_run_suffixes: activity_kind == ActivityKind::Chat
                    && parent_run_id.is_none(),
            },
            ledger_cutoff,
        };
        // The compaction boundary and the materialized history must describe
        // the same governed read. Do not reload a potentially different window
        // later in this materialization. Commit/dispatch still fence currentness.
        let dialogue_window = self
            .dialogue_window_for_source(&source, input.next_iteration)
            .await?;
        let logical_source = dialogue_window
            .as_ref()
            .map(|window| window.compaction_source())
            .transpose()
            .map_err(|_| PostgresContextMaterialError::CorruptState)?
            .unwrap_or_default();
        let sealed:Option<String> = sqlx::query_scalar("SELECT ledger_cutoff::text FROM runtime.context_compaction_slots WHERE tenant_id=$1 AND run_id=$2 AND loop_iteration=$3 AND source_key=$4 AND generation=1")
            .bind(source.run_context.tenant_id.into_uuid()).bind(source.run_context.run_id.into_uuid()).bind(i32::try_from(input.next_iteration)?).bind(logical_source.key()).fetch_optional(&self.pool).await?;
        if let Some(sealed) = sealed {
            let cutoff = parse_u64(sealed)?;
            if cutoff > source.ledger_cutoff {
                return Err(PostgresContextMaterialError::CorruptState);
            }
            source.ledger_cutoff = cutoff;
            if matches!(
                logical_source,
                musteragent_context::CompactionSource::RuntimeLedger
            ) && cutoff < ledger_head
            {
                let observed = ConversationSeq::new(ledger_head)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                let control = crate::control::consumed::latest(
                    &mut *self.pool.acquire().await?,
                    source.run_context.tenant_id,
                    source.run_context.run_id,
                    observed,
                )
                .await
                .map_err(|e| match e {
                    crate::control::consumed::ConsumedControlError::Database(e) => {
                        PostgresContextMaterialError::Database(e)
                    }
                    _ => PostgresContextMaterialError::CorruptState,
                })?;
                if let Some(control) = control.filter(|control| control.sequence.get() > cutoff) {
                    source.ledger_cutoff = control.sequence.get();
                    source.superseding_control_id = Some(control.record_id);
                }
            }
        }
        Ok((source, dialogue_window))
    }

    async fn materialize_contents<'a>(
        &self,
        tenant_id: TenantId,
        references: impl Iterator<Item = &'a ContentRef>,
    ) -> Result<Vec<MaterializedContent>, PostgresContextMaterialError> {
        let references = references.cloned().collect::<Vec<_>>();
        let payloads = self.content.read_batch(tenant_id, &references).await?;
        if payloads.len() != references.len() {
            return Err(PostgresContextMaterialError::CorruptState);
        }
        references
            .into_iter()
            .zip(payloads)
            .map(|(content_ref, bytes)| {
                verify_content(&content_ref, &bytes)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                Ok(MaterializedContent { content_ref, bytes })
            })
            .collect()
    }

    async fn materialize_execution_plan(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
    ) -> Result<Option<MaterializedExecutionPlan>, PostgresContextMaterialError> {
        let Some(row) = sqlx::query(
            "SELECT revision::text AS revision, plan_content_id AS content_id,
                    plan_media_type AS media_type,
                    plan_byte_length::text AS byte_length, plan_hash AS content_hash
             FROM runtime.execution_plans WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(run_id.into_uuid())
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let revision = row
            .try_get::<String, _>("revision")?
            .parse::<u64>()
            .ok()
            .and_then(std::num::NonZeroU64::new)
            .ok_or(PostgresContextMaterialError::CorruptState)?;
        let reference = content_ref_from_row(&row)?;
        let bytes = self.content.read(tenant_id, &reference).await?;
        verify_content(&reference, &bytes)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        Ok(Some(MaterializedExecutionPlan {
            revision,
            content: MaterializedContent {
                content_ref: reference,
                bytes,
            },
        }))
    }

    async fn materialize_messages(
        &self,
        tenant_id: TenantId,
        messages: BTreeMap<MessageId, MessageSource>,
        tool_exchange_content_ids: &BTreeSet<ContentId>,
        model_completions: &BTreeMap<InvocationId, ModelCompletionSource>,
        assistant_origins: &BTreeMap<InvocationId, AssistantOrigin>,
    ) -> Result<Vec<ContextEntry>, PostgresContextMaterialError> {
        let references = messages
            .values()
            .filter(|source| {
                source.role != CanonicalRole::Assistant
                    || !tool_exchange_content_ids.contains(&source.content_ref.content_id)
            })
            .flat_map(|source| {
                std::iter::once(source.content_ref.clone()).chain(
                    source
                        .attachments
                        .iter()
                        .filter_map(message_attachment_text_ref)
                        .cloned(),
                )
            })
            .collect::<Vec<_>>();
        let batch = ContentBatch::load(self.content.as_ref(), tenant_id, &references).await?;
        let mut entries = Vec::with_capacity(messages.len());
        for source in messages.into_values() {
            if source.role == CanonicalRole::Assistant
                && tool_exchange_content_ids.contains(&source.content_ref.content_id)
            {
                continue;
            }
            let mut content = if source.system_notice || source.partial_reason.is_some() {
                let bytes = batch.bytes(&source.content_ref)?;
                let value = std::str::from_utf8(bytes)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                ModelText::chunked(match source.partial_reason {
                    Some(reason) => {
                        musteragent_context::partial_assistant_message_text(value, reason)
                    }
                    None => format!("[System notice]\n{value}"),
                })
                .map_err(|_| PostgresContextMaterialError::CorruptState)?
                .into_iter()
                .map(CanonicalContentPart::Text)
                .collect::<Vec<_>>()
            } else {
                musteragent_context::materialize_input_body_from_batch(
                    &batch,
                    &source.content_ref,
                    source.role == CanonicalRole::User && !source.attachments.is_empty(),
                )
                .map_err(|_| PostgresContextMaterialError::CorruptState)?
            };
            // A partial response has no complete Provider replay envelope.
            // Keep it as Runtime-owned quoted evidence on every later request,
            // including after action correction succeeds or the worker restarts.
            let role = if source.partial_reason.is_some() {
                CanonicalRole::User
            } else {
                source.role
            };
            let assistant_origin = if source.partial_reason.is_some() {
                None
            } else {
                message_assistant_origin(&source, model_completions, assistant_origins)
            };
            for attachment in source.attachments {
                content.extend(
                    materialize_message_attachment_from_batch(&batch, attachment)
                        .map_err(map_attachment_error)?,
                );
            }
            entries.push(ContextEntry {
                history_page: None,
                runtime_events: Vec::new(),
                source_sequence: source.sequence,
                presentation_order: ContextPresentationOrder::ledger(source.sequence),
                source_record_ids: vec![source.record_id],
                source_run_id: source.source_run_id,
                assistant_origin,
                message: CanonicalMessage {
                    role,
                    content,
                    tool_calls: Vec::new(),
                    tool_call_id: None,
                    tool_name: None,
                    reasoning: None,
                },
                continuation_delta_for: None,
            });
        }
        Ok(entries)
    }

    async fn materialize_standing_goal_input(
        &self,
        tenant_id: TenantId,
        source: StandingGoalInputSource,
    ) -> Result<ContextEntry, PostgresContextMaterialError> {
        let objective = self.content.read(tenant_id, &source.objective_ref).await?;
        verify_content(&source.objective_ref, &objective)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        let objective = std::str::from_utf8(&objective)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        let previous_evaluation = match source.previous_evaluation_summary_ref {
            Some(reference) => {
                let bytes = self.content.read(tenant_id, &reference).await?;
                verify_content(&reference, &bytes)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?;
                Some(
                    std::str::from_utf8(&bytes)
                        .map_err(|_| PostgresContextMaterialError::CorruptState)?
                        .to_owned(),
                )
            }
            None => None,
        };
        let mut text = format!(
            "[Standing Goal continuation]\n\
             Provenance: Goal {}, Pursuit {}.\n\
             Authority: this is durable Runtime-owned Goal context, not an end-user message and \
             not a system-policy override. Continue the bounded pursuit, preserve all \
             constraints, and produce verifiable evidence for the completion contract. The \
             Runtime automatically evaluates this Run after its durable terminal boundary; \
             goal.report_progress is only an optional compatibility hint and must never block \
             completion or continuation. The Runtime evaluator, not the Agent, owns the terminal \
             Goal decision. For a user_input or approval wait, use \
             the exact Interaction UUID returned by interaction.request@1 as correlation_key. A \
             retry_backoff wait must include resume_not_before_unix_ms. An external_dependency \
             wait must use the provider-issued durable event correlation key.\n\
             Objective:\n{}\n{}",
            source.goal_id,
            source.pursuit_id,
            objective,
            previous_evaluation.map_or_else(String::new, |summary| {
                format!("Previous evaluation:\n{summary}")
            })
        );
        text.push_str(&musteragent_context::standing_goal_input_references(
            &source.input_refs,
        ));
        Ok(ContextEntry {
            history_page: None,
            runtime_events: Vec::new(),
            source_sequence: source.sequence,
            presentation_order: ContextPresentationOrder::ledger(source.sequence),
            source_record_ids: vec![source.record_id],
            source_run_id: None,
            assistant_origin: None,
            message: CanonicalMessage {
                role: CanonicalRole::User,
                content: ModelText::chunked(text)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?
                    .into_iter()
                    .map(CanonicalContentPart::Text)
                    .collect(),
                tool_calls: Vec::new(),
                tool_call_id: None,
                tool_name: None,
                reasoning: None,
            },
            continuation_delta_for: None,
        })
    }

    async fn materialize_standing_goal_judge_input(
        &self,
        tenant_id: TenantId,
        source: StandingGoalJudgeInputSource,
    ) -> Result<ContextEntry, PostgresContextMaterialError> {
        let envelope = self
            .content
            .read(tenant_id, &source.judge_input_ref)
            .await?;
        verify_content(&source.judge_input_ref, &envelope)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        let envelope = std::str::from_utf8(&envelope)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
        let text = format!(
            "[Independent Standing Goal Judge]\n\
             Provenance: Goal {}, Pursuit {}, source Run {}.\n\
             Evaluation envelope:\n{}",
            source.goal_id, source.pursuit_id, source.source_run_id, envelope,
        );
        Ok(ContextEntry {
            history_page: None,
            runtime_events: Vec::new(),
            source_sequence: source.sequence,
            presentation_order: ContextPresentationOrder::ledger(source.sequence),
            source_record_ids: vec![source.record_id],
            source_run_id: None,
            assistant_origin: None,
            message: CanonicalMessage {
                role: CanonicalRole::User,
                content: ModelText::chunked(text)
                    .map_err(|_| PostgresContextMaterialError::CorruptState)?
                    .into_iter()
                    .map(CanonicalContentPart::Text)
                    .collect(),
                tool_calls: Vec::new(),
                tool_call_id: None,
                tool_name: None,
                reasoning: None,
            },
            continuation_delta_for: None,
        })
    }

    async fn load_usage_anchor(
        &self,
        tenant_id: TenantId,
        facts: &LedgerFacts,
    ) -> Result<Option<ContextUsageAnchor>, PostgresContextMaterialError> {
        let candidates = facts
            .model_completions
            .iter()
            .filter(|(_, completion)| {
                completion
                    .run_id
                    .is_none_or(|run_id| !facts.retracted_runs.contains(&run_id))
            })
            .map(|(invocation_id, completion)| {
                let sequence = facts
                    .messages
                    .values()
                    .filter(|message| {
                        message.role == CanonicalRole::Assistant
                            && message.content_ref == completion.response_ref
                    })
                    .map(|message| message.sequence)
                    .max()
                    .unwrap_or(completion.sequence)
                    .max(completion.sequence);
                (invocation_id.into_uuid(), sequence)
            })
            .collect::<BTreeMap<_, _>>();
        if candidates.is_empty() {
            return Ok(None);
        }
        let rows = sqlx::query(
            "SELECT invocation.invocation_id, usage.total_tokens
             FROM runtime.model_invocations AS invocation
             JOIN runtime.model_usage_facts AS usage ON usage.invocation_id = invocation.invocation_id
             WHERE invocation.tenant_id = $1
               AND invocation.invocation_id = ANY($2)
               AND invocation.purpose = 1
               AND invocation.status = 2
               AND usage.total_tokens IS NOT NULL",
        )
        .bind(tenant_id.into_uuid())
        .bind(candidates.keys().copied().collect::<Vec<_>>())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let invocation_id: uuid::Uuid = row.try_get("invocation_id")?;
                let source_sequence = candidates
                    .get(&invocation_id)
                    .copied()
                    .ok_or(PostgresContextMaterialError::CorruptState)?;
                let total_tokens = u64::try_from(row.try_get::<i64, _>("total_tokens")?)?;
                Ok(ContextUsageAnchor {
                    source_sequence,
                    total_tokens,
                })
            })
            .collect::<Result<Vec<_>, PostgresContextMaterialError>>()
            .map(|anchors| {
                anchors
                    .into_iter()
                    .max_by_key(|anchor| anchor.source_sequence)
            })
    }
}

fn map_attachment_error(error: AttachmentMaterializationError) -> PostgresContextMaterialError {
    match error {
        AttachmentMaterializationError::Content(error) => {
            PostgresContextMaterialError::Content(error)
        }
        AttachmentMaterializationError::Corrupt => PostgresContextMaterialError::CorruptState,
    }
}

#[derive(Default)]
struct LedgerFacts {
    approval_observations: musteragent_context::ApprovalContextCollector,
    context_capacity_exceeded: bool,
    model_recovery: Option<InvocationId>,
    failed_primary: Option<InvocationId>,
    primary_invocation_id: Option<InvocationId>,
    retry_source_message_id: Option<MessageId>,
    consumed_control_cuts: BTreeMap<MessageId, (ConversationSeq, musteragent_run::RunControlKind)>,
    messages: BTreeMap<MessageId, MessageSource>,
    retracted_messages: BTreeSet<MessageId>,
    retracted_runs: BTreeSet<RunId>,
    model_completions: BTreeMap<InvocationId, ModelCompletionSource>,
    tool_terminals: BTreeMap<InvocationId, ToolTerminalSource>,
    branches: BTreeMap<ContextBranchId, BranchSource>,
    active_branch_id: Option<ContextBranchId>,
    records: BTreeMap<LedgerRecordId, RecordSource>,
    tool_exposures: Vec<ToolExposureAdded>,
    skill_disclosures: Vec<SkillDisclosed>,
    standing_goal_input: Option<StandingGoalInputSource>,
    coordination_input: Option<CoordinationInputSource>,
    delegation_input: Option<delegation::DelegationInput>,
    standing_goal_judge_input: Option<StandingGoalJudgeInputSource>,
    evaluation_activity_ids: BTreeSet<ActivityId>,
}

struct LedgerFactInput {
    record_id: LedgerRecordId,
    sequence: ConversationSeq,
    run_id: Option<RunId>,
    activity_id: Option<ActivityId>,
    target_activity_id: ActivityId,
    target_run_id: RunId,
    event: DomainEvent,
}

fn discovery_projection(
    snapshot: &musteragent_runtime_snapshot::RuntimeSnapshotContent,
    facts: &LedgerFacts,
    catalog: &musteragent_discovery::AuthorizedToolCatalog,
) -> Result<(Vec<ToolKey>, Vec<VersionedKey>), PostgresContextMaterialError> {
    let Some(discovery) = &snapshot.discovery else {
        return Ok((
            snapshot
                .tools
                .iter()
                .map(|tool| tool.manifest.key.clone())
                .collect(),
            snapshot
                .skills
                .iter()
                .map(|skill| skill.skill_ref.clone())
                .collect(),
        ));
    };
    let mut exposure = ExposureState::from_initial(catalog, &discovery.initial_exposed_tool_keys)
        .map_err(|_| PostgresContextMaterialError::CorruptState)?;
    for event in &facts.tool_exposures {
        exposure
            .apply(catalog, event)
            .map_err(|_| PostgresContextMaterialError::CorruptState)?;
    }
    let exposed_tool_keys = exposure
        .project(catalog)
        .into_iter()
        .map(|tool| tool.tool_ref)
        .collect();
    let mut disclosed_skill_refs = discovery.initial_disclosed_skill_refs.clone();
    let mut disclosed_skill_set = disclosed_skill_refs
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut expected_ordinal = discovery
        .initial_disclosed_skill_refs
        .len()
        .saturating_add(1);
    for event in &facts.skill_disclosures {
        let Some(skill) = snapshot
            .skills
            .iter()
            .find(|skill| skill.skill_ref == event.skill_ref)
        else {
            return Err(PostgresContextMaterialError::CorruptState);
        };
        if skill.instructions_ref != event.instructions_ref
            || skill.instructions_hash != event.instructions_hash
            || usize::try_from(event.disclosure_ordinal.get()).ok() != Some(expected_ordinal)
            || !disclosed_skill_set.insert(event.skill_ref.clone())
        {
            return Err(PostgresContextMaterialError::CorruptState);
        }
        disclosed_skill_refs.push(event.skill_ref.clone());
        expected_ordinal = expected_ordinal.saturating_add(1);
    }
    Ok((exposed_tool_keys, disclosed_skill_refs))
}

impl LedgerFacts {
    fn apply(&mut self, input: LedgerFactInput) -> Result<(), PostgresContextMaterialError> {
        let LedgerFactInput {
            record_id,
            sequence,
            run_id,
            activity_id,
            target_activity_id,
            target_run_id,
            event,
        } = input;
        if self
            .records
            .insert(
                record_id,
                RecordSource {
                    sequence,
                    branch_id: self.active_branch_id,
                },
            )
            .is_some()
        {
            return Err(PostgresContextMaterialError::CorruptState);
        }
        if activity_id.is_some_and(|candidate| {
            candidate != target_activity_id && self.evaluation_activity_ids.contains(&candidate)
        }) && matches!(
            &event,
            DomainEvent::Conversation(_)
                | DomainEvent::Model(_)
                | DomainEvent::Tool(_)
                | DomainEvent::Discovery(_)
        ) {
            return Ok(());
        }
        if run_id == Some(target_run_id) {
            match &event {
                DomainEvent::Tool(tool) => {
                    self.approval_observations
                        .observe_tool(tool, target_run_id, record_id)
                }
                DomainEvent::Interaction(interaction) => self
                    .approval_observations
                    .observe_interaction(interaction, target_run_id, record_id, sequence),
                _ => Ok(()),
            }
            .map_err(|_| {
                PostgresContextMaterialError::CorruptStateAt("approval_context_provenance")
            })?;
        }
        match event {
            DomainEvent::Model(ModelEvent::InvocationFailed {
                invocation_id,
                retry_disposition,
                ..
            }) if run_id == Some(target_run_id)
                && self.primary_invocation_id == Some(invocation_id) =>
            {
                self.failed_primary = Some(invocation_id);
                if retry_disposition
                    == musteragent_runtime_contract::RetryDisposition::RetryFallbackRoute
                {
                    self.model_recovery = Some(invocation_id);
                }
            }
            DomainEvent::Run(musteragent_run::RunEvent::RetryScheduled { error, .. })
                if run_id == Some(target_run_id) && error.retryable =>
            {
                self.context_capacity_exceeded =
                    error.code == musteragent_runtime_contract::ErrorCode::ModelContextExceeded;
                self.model_recovery = self.failed_primary;
            }
            DomainEvent::Run(musteragent_run::RunEvent::RecoveryRequested { .. })
                if run_id == Some(target_run_id) =>
            {
                self.model_recovery = self.failed_primary;
            }
            DomainEvent::Model(ModelEvent::InvocationStarted {
                purpose: musteragent_runtime_model::ModelInvocationPurpose::Primary,
                invocation_id,
                ..
            }) if run_id == Some(target_run_id) => {
                self.context_capacity_exceeded = false;
                self.model_recovery = None;
                self.failed_primary = None;
                self.primary_invocation_id = Some(invocation_id);
            }
            DomainEvent::Run(musteragent_run::RunEvent::Admitted {
                retry_source_message_id,
                ..
            }) if run_id == Some(target_run_id) => {
                self.retry_source_message_id = retry_source_message_id;
            }
            // Pending control messages are already excluded from the row set
            // by the loop_controls fence. The consumption fence itself is a
            // durable Ledger fact: it anchors the stimulus to the exact
            // boundary where it became model input, so a follow-up admitted
            // while an older invocation was in flight is presented after that
            // invocation's committed reply instead of corrupting turn order.
            DomainEvent::Run(musteragent_run::RunEvent::ControlConsumed { kind, message_ids })
                if run_id == Some(target_run_id) =>
            {
                for message_id in message_ids {
                    if self
                        .consumed_control_cuts
                        .insert(message_id, (sequence, kind))
                        .is_some()
                    {
                        return Err(PostgresContextMaterialError::CorruptState);
                    }
                }
            }
            DomainEvent::Activity(ActivityEvent::DelegationContextProjected {
                objective_ref,
                ..
            }) if activity_id == Some(target_activity_id) => {
                if self
                    .delegation_input
                    .replace(delegation::DelegationInput {
                        sequence,
                        record_id,
                        run_id: target_run_id,
                        objective_ref,
                    })
                    .is_some()
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
            }
            DomainEvent::Activity(ActivityEvent::CoordinationContextProjected {
                mailbox_message_id,
                source_conversation_id,
                payload_ref,
            }) if activity_id == Some(target_activity_id) => {
                if self
                    .coordination_input
                    .replace(CoordinationInputSource {
                        sequence,
                        record_id,
                        run_id: target_run_id,
                        source_conversation_id,
                        mailbox_message_id,
                        payload_ref,
                        branch_id: self.active_branch_id,
                    })
                    .is_some()
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
            }
            DomainEvent::Activity(ActivityEvent::StandingGoalContextProjected {
                goal_id,
                pursuit_id,
                objective_ref,
                previous_evaluation_summary_ref,
                input_refs,
            }) if activity_id == Some(target_activity_id) => {
                if self
                    .standing_goal_input
                    .replace(StandingGoalInputSource {
                        sequence,
                        record_id,
                        goal_id,
                        pursuit_id,
                        objective_ref,
                        previous_evaluation_summary_ref,
                        input_refs,
                        branch_id: self.active_branch_id,
                    })
                    .is_some()
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
            }
            DomainEvent::Activity(ActivityEvent::StandingGoalJudgeContextProjected {
                goal_id,
                pursuit_id,
                source_run_id,
                judge_input_ref,
            }) => {
                let judge_activity_id =
                    activity_id.ok_or(PostgresContextMaterialError::CorruptState)?;
                if !self.evaluation_activity_ids.insert(judge_activity_id) {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                if judge_activity_id == target_activity_id
                    && self
                        .standing_goal_judge_input
                        .replace(StandingGoalJudgeInputSource {
                            sequence,
                            record_id,
                            goal_id,
                            pursuit_id,
                            source_run_id,
                            judge_input_ref,
                            branch_id: self.active_branch_id,
                        })
                        .is_some()
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
            }
            DomainEvent::Conversation(ConversationEvent::MessageAccepted {
                message_id,
                role,
                content_ref,
                attachment_refs,
                ..
            }) => {
                if self.retracted_messages.contains(&message_id)
                    || self.messages.contains_key(&message_id)
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                let (role, system_notice) = canonical_role(role);
                self.messages.insert(
                    message_id,
                    MessageSource {
                        sequence,
                        record_id,
                        source_run_id: run_id,
                        source_invocation_id: None,
                        role,
                        system_notice,
                        partial_reason: None,
                        content_ref,
                        attachments: attachment_refs
                            .into_iter()
                            .map(|reference| reference.content_ref)
                            .collect(),
                        branch_id: self.active_branch_id,
                    },
                );
            }
            DomainEvent::Conversation(ConversationEvent::MessageCommitted {
                message_id,
                role,
                content_ref,
                content_hash,
                content_bytes,
            }) => {
                if content_ref.sha256 != content_hash
                    || content_ref.byte_length != content_bytes
                    || self.retracted_messages.contains(&message_id)
                    || self.messages.contains_key(&message_id)
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                let (role, system_notice) = canonical_role(role);
                let source_invocation_id = (role == CanonicalRole::Assistant)
                    .then(|| latest_completion_for_run(&self.model_completions, run_id, sequence))
                    .flatten();
                self.messages.insert(
                    message_id,
                    MessageSource {
                        sequence,
                        record_id,
                        source_run_id: run_id,
                        source_invocation_id,
                        role,
                        system_notice,
                        partial_reason: None,
                        content_ref,
                        attachments: Vec::new(),
                        branch_id: self.active_branch_id,
                    },
                );
            }
            DomainEvent::Conversation(ConversationEvent::MessagePartiallyCommitted {
                message_id,
                content_ref,
                content_hash,
                content_bytes,
                reason,
            }) => {
                if content_ref.sha256 != content_hash
                    || content_ref.byte_length != content_bytes
                    || self.retracted_messages.contains(&message_id)
                    || self.messages.contains_key(&message_id)
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                self.messages.insert(
                    message_id,
                    MessageSource {
                        sequence,
                        record_id,
                        source_run_id: run_id,
                        source_invocation_id: None,
                        role: CanonicalRole::Assistant,
                        system_notice: false,
                        partial_reason: Some(reason),
                        content_ref,
                        attachments: Vec::new(),
                        branch_id: self.active_branch_id,
                    },
                );
            }
            DomainEvent::Conversation(ConversationEvent::MessageImported {
                message_id,
                role,
                content_ref,
                attachment_refs,
                ..
            }) => {
                if self.retracted_messages.contains(&message_id)
                    || self.messages.contains_key(&message_id)
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                let (role, system_notice) = canonical_role(role);
                self.messages.insert(
                    message_id,
                    MessageSource {
                        sequence,
                        record_id,
                        source_run_id: run_id,
                        source_invocation_id: None,
                        role,
                        system_notice,
                        partial_reason: None,
                        content_ref,
                        attachments: attachment_refs
                            .into_iter()
                            .map(|reference| reference.content_ref)
                            .collect(),
                        branch_id: self.active_branch_id,
                    },
                );
            }
            DomainEvent::Conversation(ConversationEvent::MessageRetracted {
                message_id,
                source_run_id,
                ..
            }) => {
                let source_run_id = source_run_id.or_else(|| {
                    self.messages
                        .get(&message_id)
                        .and_then(|message| message.source_run_id)
                });
                self.messages.remove(&message_id);
                self.retracted_messages.insert(message_id);
                if let Some(run_id) = source_run_id {
                    self.retracted_runs.insert(run_id);
                }
            }
            DomainEvent::Model(ModelEvent::InvocationCompleted {
                invocation_id,
                response_ref,
                ..
            }) => {
                if self
                    .model_completions
                    .insert(
                        invocation_id,
                        ModelCompletionSource {
                            sequence,
                            record_id,
                            run_id,
                            response_ref,
                            branch_id: self.active_branch_id,
                        },
                    )
                    .is_some()
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
            }
            DomainEvent::Tool(ToolEvent::InvocationIndeterminate {
                invocation_id,
                diagnostic_result_ref,
                diagnostic_result_hash,
                reason,
                observed_error,
                ..
            }) => {
                if diagnostic_result_ref
                    .as_ref()
                    .map(|reference| reference.sha256)
                    != diagnostic_result_hash
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                if self
                    .tool_terminals
                    .insert(
                        invocation_id,
                        ToolTerminalSource {
                            sequence,
                            record_id,
                            outcome: ToolTerminalSourceKind::Indeterminate {
                                diagnostic_result_ref,
                                reason,
                                observed_error,
                            },
                            branch_id: self.active_branch_id,
                        },
                    )
                    .is_some_and(|previous| {
                        !matches!(
                            previous.outcome,
                            ToolTerminalSourceKind::Indeterminate { .. }
                        ) || previous.branch_id != self.active_branch_id
                    })
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
            }
            DomainEvent::Tool(ToolEvent::InvocationSucceeded {
                invocation_id,
                result_ref,
                result_hash,
                ..
            }) => {
                if result_ref.sha256 != result_hash
                    || self
                        .tool_terminals
                        .insert(
                            invocation_id,
                            ToolTerminalSource {
                                sequence,
                                record_id,
                                outcome: ToolTerminalSourceKind::Succeeded { result_ref },
                                branch_id: self.active_branch_id,
                            },
                        )
                        .is_some_and(|previous| {
                            !matches!(
                                previous.outcome,
                                ToolTerminalSourceKind::Indeterminate { .. }
                            ) || previous.branch_id != self.active_branch_id
                        })
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
            }
            DomainEvent::Tool(ToolEvent::InvocationFailed {
                invocation_id,
                diagnostic_result_ref,
                diagnostic_result_hash,
                ..
            }) => {
                let diagnostic_result_ref = match (diagnostic_result_ref, diagnostic_result_hash) {
                    (None, None) => None,
                    (Some(reference), Some(hash)) if reference.sha256 == hash => Some(reference),
                    _ => {
                        return Err(PostgresContextMaterialError::CorruptStateAt(
                            "tool_terminal_diagnostic_event",
                        ));
                    }
                };
                if self
                    .tool_terminals
                    .insert(
                        invocation_id,
                        ToolTerminalSource {
                            sequence,
                            record_id,
                            outcome: ToolTerminalSourceKind::Failed {
                                diagnostic_result_ref,
                            },
                            branch_id: self.active_branch_id,
                        },
                    )
                    .is_some_and(|previous| {
                        !matches!(
                            previous.outcome,
                            ToolTerminalSourceKind::Indeterminate { .. }
                        ) || previous.branch_id != self.active_branch_id
                    })
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
            }
            DomainEvent::Tool(ToolEvent::InvocationCancelled { invocation_id, .. }) => {
                if self
                    .tool_terminals
                    .insert(
                        invocation_id,
                        ToolTerminalSource {
                            sequence,
                            record_id,
                            outcome: ToolTerminalSourceKind::Cancelled,
                            branch_id: self.active_branch_id,
                        },
                    )
                    .is_some_and(|previous| {
                        !matches!(
                            previous.outcome,
                            ToolTerminalSourceKind::Indeterminate { .. }
                        ) || previous.branch_id != self.active_branch_id
                    })
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
            }
            DomainEvent::Context(musteragent_context::ContextEvent::BranchCreated {
                branch_id,
                parent_branch_id,
                fork_from_record_id,
                ..
            }) => {
                if parent_branch_id != self.active_branch_id
                    || self.branches.contains_key(&branch_id)
                    || !self.record_reachable_from(fork_from_record_id, self.active_branch_id)?
                {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                let fork_sequence = self
                    .records
                    .get(&fork_from_record_id)
                    .map(|record| record.sequence.get())
                    .ok_or(PostgresContextMaterialError::CorruptState)?;
                self.branches.insert(
                    branch_id,
                    BranchSource {
                        parent_branch_id,
                        fork_sequence,
                    },
                );
            }
            DomainEvent::Context(musteragent_context::ContextEvent::BranchActivated {
                branch_id,
                ..
            }) => {
                if !self.branches.contains_key(&branch_id) {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                self.active_branch_id = Some(branch_id);
            }
            DomainEvent::Context(musteragent_context::ContextEvent::Derived {
                branch_id, ..
            }) => {
                if !self.branches.is_empty() || self.active_branch_id.is_some() {
                    return Err(PostgresContextMaterialError::CorruptState);
                }
                self.branches.insert(
                    branch_id,
                    BranchSource {
                        parent_branch_id: None,
                        fork_sequence: 0,
                    },
                );
                self.active_branch_id = Some(branch_id);
            }
            DomainEvent::Discovery(DiscoveryEvent::ToolExposureAdded(event))
                if run_id == Some(target_run_id) =>
            {
                self.tool_exposures.push(event);
            }
            DomainEvent::Discovery(DiscoveryEvent::SkillDisclosed(event))
                if run_id == Some(target_run_id) =>
            {
                self.skill_disclosures.push(event);
            }
            _ => {}
        }
        Ok(())
    }

    fn retain_active_branch(&mut self) -> Result<(), PostgresContextMaterialError> {
        let limits = self.branch_limits(self.active_branch_id)?;
        let reachable = |branch_id: Option<ContextBranchId>, sequence: ConversationSeq| {
            limits
                .get(&branch_id)
                .is_some_and(|limit| sequence.get() <= *limit)
        };
        self.messages
            .retain(|_, source| reachable(source.branch_id, source.sequence));
        self.model_completions
            .retain(|_, source| reachable(source.branch_id, source.sequence));
        self.tool_terminals
            .retain(|_, source| reachable(source.branch_id, source.sequence));
        if self
            .coordination_input
            .as_ref()
            .is_some_and(|source| !reachable(source.branch_id, source.sequence))
        {
            self.coordination_input = None;
        }
        if self
            .standing_goal_input
            .as_ref()
            .is_some_and(|source| !reachable(source.branch_id, source.sequence))
        {
            self.standing_goal_input = None;
        }
        if self
            .standing_goal_judge_input
            .as_ref()
            .is_some_and(|source| !reachable(source.branch_id, source.sequence))
        {
            self.standing_goal_judge_input = None;
        }
        Ok(())
    }

    fn record_reachable_from(
        &self,
        record_id: LedgerRecordId,
        target: Option<ContextBranchId>,
    ) -> Result<bool, PostgresContextMaterialError> {
        let record = self
            .records
            .get(&record_id)
            .ok_or(PostgresContextMaterialError::CorruptState)?;
        Ok(self
            .branch_limits(target)?
            .get(&record.branch_id)
            .is_some_and(|limit| record.sequence.get() <= *limit))
    }

    fn active_branch_includes(
        &self,
        branch_id: Option<ContextBranchId>,
        sequence: ConversationSeq,
    ) -> Result<bool, PostgresContextMaterialError> {
        Ok(self
            .branch_limits(self.active_branch_id)?
            .get(&branch_id)
            .is_some_and(|limit| sequence.get() <= *limit))
    }

    fn branch_limits(
        &self,
        target: Option<ContextBranchId>,
    ) -> Result<BTreeMap<Option<ContextBranchId>, u64>, PostgresContextMaterialError> {
        let mut limits = BTreeMap::new();
        let mut current = target;
        let mut limit = u64::MAX;
        loop {
            if limits.insert(current, limit).is_some() {
                return Err(PostgresContextMaterialError::CorruptState);
            }
            let Some(branch_id) = current else {
                break;
            };
            let branch = self
                .branches
                .get(&branch_id)
                .ok_or(PostgresContextMaterialError::CorruptState)?;
            limit = limit.min(branch.fork_sequence);
            current = branch.parent_branch_id;
        }
        Ok(limits)
    }
}

struct BranchSource {
    parent_branch_id: Option<ContextBranchId>,
    fork_sequence: u64,
}

struct RecordSource {
    sequence: ConversationSeq,
    branch_id: Option<ContextBranchId>,
}

struct MessageSource {
    sequence: ConversationSeq,
    record_id: LedgerRecordId,
    source_run_id: Option<RunId>,
    source_invocation_id: Option<InvocationId>,
    role: CanonicalRole,
    system_notice: bool,
    partial_reason: Option<musteragent_conversation::PartialMessageReason>,
    content_ref: ContentRef,
    attachments: Vec<ContentRef>,
    branch_id: Option<ContextBranchId>,
}

struct StandingGoalInputSource {
    sequence: ConversationSeq,
    record_id: LedgerRecordId,
    goal_id: GoalId,
    pursuit_id: GoalPursuitId,
    objective_ref: ContentRef,
    previous_evaluation_summary_ref: Option<ContentRef>,
    input_refs: Vec<ContentRef>,
    branch_id: Option<ContextBranchId>,
}

struct StandingGoalJudgeInputSource {
    sequence: ConversationSeq,
    record_id: LedgerRecordId,
    goal_id: GoalId,
    pursuit_id: GoalPursuitId,
    source_run_id: RunId,
    judge_input_ref: ContentRef,
    branch_id: Option<ContextBranchId>,
}

#[derive(Clone)]
struct ModelCompletionSource {
    sequence: ConversationSeq,
    record_id: LedgerRecordId,
    run_id: Option<RunId>,
    response_ref: ContentRef,
    branch_id: Option<ContextBranchId>,
}

struct ToolExchangeSource {
    response_ref: ContentRef,
    completion: ModelCompletionSource,
    reasoning: Option<ReasoningReplay>,
    calls: Vec<CanonicalToolCall>,
    results: Vec<ToolResultSource>,
}

#[derive(Default)]
struct MaterializedToolExchanges {
    entries: Vec<ContextEntry>,
    assistant_content_ids: BTreeSet<ContentId>,
}

struct ToolResultSource {
    tool_key: String,
    content_ref: ContentRef,
    sequence: ConversationSeq,
    record_id: LedgerRecordId,
    source_ordinal: u32,
    expected_call_id: ToolCallId,
    expected_name: WireName,
}

#[cfg(test)]
#[path = "context_material_tests.rs"]
mod tests;

fn application_material_error(
    error: musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialError,
) -> PostgresContextMaterialError {
    match error {
        musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialError::Unavailable => PostgresContextMaterialError::Content(musteragent_runtime_content::ContentStoreError::Unavailable),
        _ => PostgresContextMaterialError::CorruptState,
    }
}
