use std::collections::BTreeSet;
use std::sync::Arc;

use musteragent_activity::{ActivityEvent, ActivityKind};
use musteragent_artifact::ArtifactEvent;
use musteragent_content_ref::Sha256;
use musteragent_context::RuntimeSnapshotDecoder;
use musteragent_conversation::{ConversationEvent, MessageRole};
use musteragent_discovery::{AuthorizedToolCatalog, DiscoveryEvent, ExposureState};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_execution_grant::EncodedExecutionGrantProof;
use musteragent_identity::{
    ActivityId, Actor, AgentId, ConversationId, DelegationId, DeviceId, HomeEpoch, ParticipantId,
    PrincipalId, PrincipalKind, RunId, RuntimeSnapshotId, TenantId, WorkspaceId,
};
use musteragent_ledger::DomainEvent;
use musteragent_policy::{ApprovalRequirement, RequiredContext};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_run::{AttemptContext, RunContext};
use musteragent_runtime_capability::{
    InvocationAuthorization, InvocationPolicyBasis, TaskProvenanceRecord, TaskProvenanceSourceKind,
    ToolApprovalPosture, ToolConsentAuthority, ToolExecutionMaterial, ToolExecutionMaterialSource,
    ToolExecutionPortError,
};
use musteragent_runtime_contract::{ApplicationFuture, ErrorCode};

mod initial_activity;
mod original_receipt;
use musteragent_tool::{ToolEvent, WireName};
use sqlx::{PgPool, Row};

use crate::run_ledger::{RunLedgerReadError, load_run_domain_events};

struct ToolMaterialScope {
    tenant_id: TenantId,
    run_id: RunId,
    activity_id: ActivityId,
    conversation_id: ConversationId,
}

#[derive(Clone)]
pub struct PostgresToolExecutionMaterialSource {
    application_catalogs: Option<
        Arc<
            dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource,
        >,
    >,
    pool: PgPool,
    snapshots: Arc<dyn RuntimeSnapshotDecoder>,
    ledger_decoder: Arc<dyn LedgerPayloadDecoder>,
}

impl PostgresToolExecutionMaterialSource {
    pub fn with_application_catalogs(
        mut self,
        source: Option<Arc<dyn musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialSource>>,
    ) -> Self {
        self.application_catalogs = source;
        self
    }

    #[must_use]
    pub fn new(
        pool: PgPool,
        snapshots: Arc<dyn RuntimeSnapshotDecoder>,
        ledger_decoder: Arc<dyn LedgerPayloadDecoder>,
    ) -> Self {
        Self {
            application_catalogs: None,
            pool,
            snapshots,
            ledger_decoder,
        }
    }

    async fn load_inner(
        &self,
        attempt: &AttemptContext,
        wire_name: &WireName,
    ) -> Result<ToolExecutionMaterial, MaterialError> {
        let source = self.load_run(attempt).await?;
        let snapshot = self
            .snapshots
            .decode_runtime_snapshot(&source.encoded_snapshot)
            .map_err(|_| MaterialError::Corrupt)?;
        snapshot
            .validate_sealed()
            .map_err(|_| MaterialError::Corrupt)?;
        if source
            .owned_agent_id
            .is_some_and(|agent| agent != snapshot.agent.agent_id)
        {
            return Err(MaterialError::Corrupt);
        }
        let resolved = musteragent_runtime_snapshot::application_catalog::resolve_tool_catalog(
            &snapshot,
            self.application_catalogs.as_deref(),
        )
        .await
        .map_err(MaterialError::application)?;
        snapshot
            .validate_resolved_tool_catalog(&resolved.catalog)
            .map_err(|_| MaterialError::Corrupt)?;
        let authorized_tools = resolved.catalog.clone();
        let initial_exposed = snapshot.discovery.as_ref().map_or_else(
            || {
                snapshot
                    .tools
                    .iter()
                    .map(|tool| tool.manifest.key.clone())
                    .collect()
            },
            |discovery| discovery.initial_exposed_tool_keys.clone(),
        );
        let ledger_material = self
            .load_run_ledger_material(
                ToolMaterialScope {
                    tenant_id: source.run_context.tenant_id,
                    run_id: source.run_context.run_id,
                    activity_id: source.run_context.activity_id,
                    conversation_id: source.run_context.conversation_id,
                },
                &authorized_tools,
                &initial_exposed,
                source.dialogue_input.as_ref(),
            )
            .await?;
        let key = authorized_tools
            .get_by_wire_name(wire_name.as_str())
            .ok_or(MaterialError::ToolNotPinned)?
            .tool_ref
            .clone();
        let tool = resolved
            .executable(&snapshot, self.application_catalogs.as_deref(), &key)
            .await
            .map_err(MaterialError::application)?;
        if ledger_material
            .exposure_state
            .ordinal_for(&tool.manifest.key)
            .is_none()
        {
            return Err(MaterialError::ToolNotExposed);
        }
        let capability_grant = snapshot
            .capabilities
            .iter()
            .find(|grant| {
                grant.key.as_str() == tool.manifest.capability_key.as_str()
                    && grant.provider_id == tool.manifest.capability_provider_id
            })
            .cloned()
            .ok_or(MaterialError::Corrupt)?;
        let mut authorized_content = crate::PostgresContextMaterialSource::visible_attachment_refs(
            &self.pool,
            self.ledger_decoder.as_ref(),
            &source.run_context,
            snapshot.context_policy.effective_source_selection(),
        )
        .await
        .map_err(|error| match error {
            crate::PostgresContextMaterialError::Database(error) => MaterialError::Database(error),
            _ => MaterialError::Corrupt,
        })?;
        let reads_content = tool.manifest.key.as_str() == "context.read@2";
        let private_result_refs = if reads_content
            && snapshot
                .prompt
                .sections
                .iter()
                .any(|s| s.section_key.as_str() == "dialogue.previous-result")
        {
            let mut conn = self.pool.acquire().await.map_err(MaterialError::Database)?;
            crate::work_item_execution::requester::load_feedback(
                &mut conn,
                source.run_context.tenant_id,
                source.run_context.run_id,
            )
            .await
            .map_err(|_| MaterialError::SourceAuthority)?
            .ok_or(MaterialError::SourceAuthority)?
            .files
        } else if reads_content {
            crate::work_item_execution::result_receipts::report_evidence(
                &self.pool,
                source.run_context.tenant_id,
                source.run_context.run_id,
            )
            .await
            .map_err(|_| MaterialError::SourceAuthority)?
        } else {
            Vec::new()
        };
        for reference in private_result_refs
            .into_iter()
            .chain(ledger_material.attachments)
            .chain(
                source
                    .dialogue_input
                    .as_ref()
                    .into_iter()
                    .flat_map(|s| s.reply_content.iter().cloned()),
            )
        {
            match authorized_content
                .iter()
                .find(|existing| existing.content_id == reference.content_id)
            {
                Some(existing) if existing != &reference => return Err(MaterialError::Corrupt),
                Some(_) => {}
                None => authorized_content.push(reference),
            }
        }
        let mut available_contexts = BTreeSet::new();
        if matches!(
            source.run_context.execution_scope,
            ExecutionScope::Workspace { .. }
                | ExecutionScope::DevicePrivate {
                    workspace_id: Some(_),
                    ..
                }
        ) {
            available_contexts.insert(RequiredContext::Workspace);
        }
        // The frozen device grant binds these file operations to the original
        // installation's registered default workspace. Other tools retain the Run scope.
        if snapshot
            .product_release
            .as_ref()
            .is_some_and(|release| release.product == musteragent_identity::ProductId::KERNEL)
            && capability_grant.location == musteragent_capability::CapabilityLocation::Device
            && matches!(
                source.run_context.execution_scope,
                ExecutionScope::Tenant { .. }
            )
            && musteragent_tool::origin_device_tool(tool.manifest.key.as_str())
        {
            available_contexts.insert(RequiredContext::Workspace);
        }
        if !authorized_content.is_empty() {
            available_contexts.insert(RequiredContext::ConversationAttachments);
        }
        let policy_requires_approval = tool.manifest.approval == ApprovalRequirement::ByPolicy
            && snapshot
                .policy
                .permission_mode
                .defaults_by_policy_to_approval();
        let policy_basis =
            (!ledger_material.current_task_records.is_empty()).then(|| InvocationPolicyBasis {
                source_records: ledger_material.current_task_records,
                agent_policy_ref: snapshot.policy.policy_ref.clone(),
                agent_policy_hash: snapshot.policy.policy_hash,
                permission_mode: snapshot.policy.permission_mode,
            });
        let approval = ToolApprovalPosture::resolve(
            &tool.manifest,
            snapshot.policy.permission_mode,
            if let Some(input) = source.dialogue_input.as_ref() {
                input.kind.dialogue_consent_authority()
            } else if source.run_context.activity_kind == musteragent_activity::ActivityKind::Chat {
                ToolConsentAuthority::InteractiveChat
            } else {
                ToolConsentAuthority::ExecutionSurface
            },
        );
        Ok(ToolExecutionMaterial {
            application_capabilities: if snapshot.application_catalogs.is_empty() {
                vec![]
            } else {
                snapshot.capabilities.clone()
            },
            product_release: snapshot.product_release.clone(),
            model_liveness: snapshot
                .execution_policy
                .liveness
                .map(|value| Arc::new(value.model_policy())),
            recovery_policy: snapshot.execution_policy.recovery_policy,
            run_context: source.run_context,
            actor: source.actor,
            agent_id: snapshot.agent.agent_id,
            model: snapshot.model,
            active_model_route_ordinal: source.active_model_route_ordinal,
            auxiliary_models: snapshot.auxiliary_models,
            model_stream_budget: snapshot.execution_policy.model_stream,
            authorized_tools,
            discovery: snapshot.discovery,
            exposure_state: ledger_material.exposure_state,
            authorization: InvocationAuthorization {
                actor_scopes: capability_grant.allowed_scopes.clone(),
                available_contexts,
                policy_requires_approval,
                approval_forbidden: approval.approval_forbidden,
                manifest_approval_not_applicable: approval.manifest_approval_not_applicable,
                approval_satisfied: false,
            },
            permission_mode: snapshot.policy.permission_mode,
            policy_basis,
            tool,
            capability_grant,
            authorized_content,
            prior_tool_results: ledger_material.prior_tool_results,
            execution_grant_proof: source.execution_grant_proof,
        })
    }

    async fn load_run(&self, attempt: &AttemptContext) -> Result<RunMaterialSource, MaterialError> {
        let mut conn = self.pool.acquire().await?;
        let row = sqlx::query(
            "SELECT run.tenant_id, run.conversation_id, run.activity_id,
                    run.activity_kind, run.executor_participant_id,
                    run.requested_by_participant_id, run.parent_run_id,
                    run.delegation_id, run.execution_scope_kind,
                    run.workspace_id, run.device_id, run.control_home_kind,
                    run.control_device_id, run.execution_home_kind,
                    run.execution_device_id,
                    run.home_epoch::text AS home_epoch,
                    run.runtime_snapshot_id, checkpoint.active_model_route_ordinal,
                    snapshot.encoded_snapshot,
                    snapshot.execution_grant_proof_schema, snapshot.execution_grant_proof,
                    snapshot.execution_grant_proof_hash, snapshot.execution_grant_claims_hash,
                    snapshot.content_hash, conversation.purpose AS conversation_purpose, dialogue_session.source_intent_id,
                    CASE WHEN conversation.purpose <> 'standard'
                      THEN dialogue_session.agent_id ELSE NULL END AS owned_agent_id,
                    CASE WHEN conversation.purpose <> 'standard'
                      THEN dialogue_user.principal_ref ELSE participant.principal_ref END AS principal_ref
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
             JOIN runtime.loop_checkpoints AS checkpoint
               ON checkpoint.tenant_id = run.tenant_id
              AND checkpoint.run_id = run.run_id
              AND checkpoint.attempt_id = attempt.attempt_id
             JOIN conversation.participants AS participant
               ON participant.tenant_id = run.tenant_id
              AND participant.conversation_id = run.conversation_id
              AND participant.participant_id = run.requested_by_participant_id
              AND participant.left_at_ms IS NULL
             JOIN conversation.conversations conversation
               ON conversation.tenant_id=run.tenant_id AND conversation.conversation_id=run.conversation_id
             LEFT JOIN agent.dialogue_sessions dialogue_session
               ON dialogue_session.tenant_id=run.tenant_id AND dialogue_session.conversation_id=run.conversation_id
               AND dialogue_session.owner_id=conversation.created_by AND dialogue_session.conversation_purpose=conversation.purpose
             LEFT JOIN agent.owned_agents agent
               ON agent.agent_id=dialogue_session.agent_id AND agent.dialogue_id=dialogue_session.dialogue_id
             LEFT JOIN agent.owner_spaces owner_space
               ON owner_space.space_id=agent.space_id AND owner_space.tenant_id=dialogue_session.tenant_id
               AND owner_space.owner_id=dialogue_session.owner_id
             LEFT JOIN conversation.participants dialogue_user
               ON dialogue_user.tenant_id=dialogue_session.tenant_id AND dialogue_user.conversation_id=dialogue_session.conversation_id
               AND dialogue_user.kind=1 AND dialogue_user.principal_ref=dialogue_session.owner_id
               AND dialogue_user.left_at_ms IS NULL
             LEFT JOIN conversation.participants dialogue_executor
               ON dialogue_executor.tenant_id=dialogue_session.tenant_id AND dialogue_executor.conversation_id=dialogue_session.conversation_id
               AND dialogue_executor.participant_id=run.executor_participant_id AND dialogue_executor.kind=2
               AND dialogue_executor.agent_ref=dialogue_session.agent_id AND dialogue_executor.left_at_ms IS NULL
             WHERE run.run_id = $1
               AND ((conversation.purpose='standard' AND participant.kind=1)
                 OR (conversation.purpose IN ('dialogue_intent_coordination','agent_work_item_execution','agent_work_item_report','owner_goal_execution') AND conversation.kind=4
                   AND conversation.workspace_id IS NULL AND conversation.lifecycle=1
                   AND participant.kind=3 AND ((dialogue_session.subject_kind='intent' AND participant.service_ref='musteragent.dialogue-intent@1') OR (dialogue_session.subject_kind='work_item' AND participant.service_ref='musteragent.work-item@1') OR (dialogue_session.subject_kind='work_item_report' AND participant.service_ref='musteragent.work-item-report@1') OR (dialogue_session.subject_kind='goal' AND participant.service_ref='musteragent.owner-goal@1'))
                   AND dialogue_session.purpose IN ('intent_coordination','work_item_execution','work_item_report','goal_execution') AND agent.status='active' AND owner_space.space_id IS NOT NULL
                   AND dialogue_user.participant_id IS NOT NULL AND dialogue_executor.participant_id IS NOT NULL))
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
        .bind(i32::try_from(attempt.attempt_number.get()).map_err(|_| MaterialError::Corrupt)?)
        .bind(attempt.worker_id.as_str())
        .bind(attempt.lease_id.into_uuid())
        .bind(attempt.fencing_token.get().to_string())
        .fetch_optional(&mut *conn)
        .await?
        .ok_or(MaterialError::StaleFence)?;
        let encoded_snapshot: Vec<u8> = row.try_get("encoded_snapshot")?;
        let persisted_hash =
            musteragent_content_ref::Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                .map_err(|_| MaterialError::Corrupt)?;
        if musteragent_content_ref::Sha256::digest(&encoded_snapshot) != persisted_hash {
            return Err(MaterialError::Corrupt);
        }
        let tenant_id: TenantId = id(row.try_get("tenant_id")?)?;
        let run_context = RunContext {
            tenant_id,
            conversation_id: id(row.try_get("conversation_id")?)?,
            activity_id: id(row.try_get("activity_id")?)?,
            activity_kind: activity_kind(row.try_get("activity_kind")?)?,
            run_id: attempt.run_id,
            executor_participant_id: id(row.try_get("executor_participant_id")?)?,
            requested_by_participant_id: id(row.try_get("requested_by_participant_id")?)?,
            parent_run_id: optional_id(row.try_get("parent_run_id")?)?,
            delegation_id: optional_id(row.try_get("delegation_id")?)?,
            execution_scope: execution_scope(
                row.try_get("execution_scope_kind")?,
                tenant_id,
                row.try_get("workspace_id")?,
                row.try_get("device_id")?,
            )?,
            control_home: runtime_home(
                row.try_get("control_home_kind")?,
                row.try_get("control_device_id")?,
            )?,
            execution_home: runtime_home(
                row.try_get("execution_home_kind")?,
                row.try_get("execution_device_id")?,
            )?,
            home_epoch: HomeEpoch::new(parse_u64(row.try_get("home_epoch")?)?)
                .map_err(|_| MaterialError::Corrupt)?,
            runtime_snapshot_id: id(row.try_get("runtime_snapshot_id")?)?,
        };
        let principal_id: PrincipalId = id(row
            .try_get::<Option<uuid::Uuid>, _>("principal_ref")?
            .ok_or(MaterialError::Corrupt)?)?;
        let owned_agent_id = row
            .try_get::<Option<uuid::Uuid>, _>("owned_agent_id")?
            .map(AgentId::from_uuid)
            .transpose()
            .map_err(|_| MaterialError::Corrupt)?;
        let actor = Actor {
            tenant_id,
            principal_id,
            principal_kind: PrincipalKind::User,
        };
        let purpose = musteragent_conversation::ConversationPurpose::from_storage(
            row.try_get::<&str, _>("conversation_purpose")?,
        )
        .ok_or(MaterialError::Corrupt)?;
        let intent_id = row
            .try_get::<Option<uuid::Uuid>, _>("source_intent_id")?
            .map(musteragent_identity::UserIntentId::from_uuid)
            .transpose()
            .map_err(|_| MaterialError::Corrupt)?;
        let dialogue_input = if purpose != musteragent_conversation::ConversationPurpose::Standard {
            Some(
                crate::dialogue_input_provenance::resolve_bound(
                    &mut conn,
                    actor,
                    purpose,
                    owned_agent_id,
                    intent_id,
                    run_context.delegation_id.is_some(),
                )
                .await
                .map_err(MaterialError::dialogue_source)?
                .ok_or(MaterialError::Corrupt)?,
            )
        } else {
            None
        };
        Ok(RunMaterialSource {
            owned_agent_id,
            dialogue_input,
            run_context,
            actor,
            encoded_snapshot,
            execution_grant_proof: crate::execution_grant_proof::from_row(&row).map_err(
                |error| match error {
                    crate::execution_grant_proof::StoredGrantProofError::Database(error) => {
                        MaterialError::Database(error)
                    }
                    crate::execution_grant_proof::StoredGrantProofError::Corrupt => {
                        MaterialError::Corrupt
                    }
                },
            )?,
            active_model_route_ordinal: u32::try_from(
                row.try_get::<i32, _>("active_model_route_ordinal")?,
            )
            .map_err(|_| MaterialError::Corrupt)?,
        })
    }

    async fn load_run_ledger_material(
        &self,
        ToolMaterialScope {
            tenant_id,
            run_id,
            activity_id,
            conversation_id,
        }: ToolMaterialScope,
        catalog: &AuthorizedToolCatalog,
        initial_exposed: &[musteragent_tool::ToolKey],
        dialogue_input: Option<&crate::dialogue_input_provenance::DialogueRunInput>,
    ) -> Result<RunLedgerMaterial, MaterialError> {
        let records =
            load_run_domain_events(&self.pool, self.ledger_decoder.as_ref(), tenant_id, run_id)
                .await
                .map_err(MaterialError::from)?;
        // The admission MessageAccepted record is intentionally activity-bound
        // and has no run_id. Tool exposure and invocation records are Run-bound.
        // Message attachment authority comes exclusively from the shared visible
        // conversation reader. Here retain only explicit activity/tool grants.
        let initial = self
            .load_initial_activity_material(tenant_id, conversation_id, activity_id, dialogue_input)
            .await?;
        let mut attachments = initial.attachments;
        let mut prior_tool_results = Vec::new();
        let mut current_task_records = initial.current_task_records;
        let mut seen = attachments
            .iter()
            .map(|attachment| attachment.content_id)
            .collect::<BTreeSet<_>>();
        let mut seen_tool_results = BTreeSet::new();
        let mut exposure_state = ExposureState::from_initial(catalog, initial_exposed)
            .map_err(|_| MaterialError::Corrupt)?;
        for record in records {
            let record_id = record.record_id;
            match record.event {
                DomainEvent::Conversation(ConversationEvent::MessageAccepted {
                    role,
                    content_ref,
                    ..
                }) => {
                    if role == MessageRole::User {
                        current_task_records.push(TaskProvenanceRecord {
                            record_id,
                            source_kind: dialogue_input
                                .map(|input| input.accepted_kind(&content_ref))
                                .transpose()
                                .map_err(MaterialError::dialogue_source)?
                                .unwrap_or(TaskProvenanceSourceKind::UserMessage),
                            content_ref,
                        });
                    }
                }
                DomainEvent::Conversation(ConversationEvent::MessageImported {
                    role,
                    content_ref,
                    ..
                }) => {
                    if role == MessageRole::User {
                        current_task_records.push(TaskProvenanceRecord {
                            record_id,
                            source_kind: TaskProvenanceSourceKind::ImportedUserMessage,
                            content_ref,
                        });
                    }
                }
                DomainEvent::Activity(ActivityEvent::DelegationContextProjected {
                    objective_ref,
                    ..
                }) => {
                    current_task_records.push(TaskProvenanceRecord {
                        record_id,
                        source_kind: TaskProvenanceSourceKind::DelegatedObjective,
                        content_ref: objective_ref,
                    });
                }
                DomainEvent::Activity(ActivityEvent::StandingGoalContextProjected {
                    objective_ref,
                    input_refs,
                    ..
                }) => {
                    current_task_records.push(TaskProvenanceRecord {
                        record_id,
                        source_kind: TaskProvenanceSourceKind::StandingGoalObjective,
                        content_ref: objective_ref,
                    });
                    for reference in input_refs {
                        if seen.insert(reference.content_id) {
                            attachments.push(reference);
                        }
                    }
                }
                DomainEvent::Discovery(DiscoveryEvent::ToolExposureAdded(event)) => {
                    exposure_state
                        .apply(catalog, &event)
                        .map_err(|_| MaterialError::Corrupt)?;
                }
                DomainEvent::Tool(ToolEvent::InvocationSucceeded {
                    result_ref,
                    authorized_content,
                    ..
                }) => {
                    if seen_tool_results.insert(result_ref.content_id) {
                        prior_tool_results.push(result_ref);
                    }
                    for reference in authorized_content {
                        if seen.insert(reference.content_id) {
                            attachments.push(reference);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(RunLedgerMaterial {
            attachments,
            prior_tool_results,
            current_task_records,
            exposure_state,
        })
    }

    async fn load_initial_activity_material(
        &self,
        tenant_id: TenantId,
        conversation_id: ConversationId,
        activity_id: ActivityId,
        dialogue_input: Option<&crate::dialogue_input_provenance::DialogueRunInput>,
    ) -> Result<InitialActivityMaterial, MaterialError> {
        let events = self
            .load_initial_activity_events(tenant_id, conversation_id, activity_id)
            .await?;
        let mut attachments = Vec::new();
        let mut current_task_records = Vec::new();
        let mut seen = BTreeSet::new();
        for (record_id, event) in events {
            match event {
                DomainEvent::Conversation(ConversationEvent::MessageAccepted {
                    role,
                    content_ref,
                    ..
                }) => {
                    if role == MessageRole::User {
                        current_task_records.push(TaskProvenanceRecord {
                            record_id,
                            source_kind: dialogue_input
                                .map(|input| input.accepted_kind(&content_ref))
                                .transpose()
                                .map_err(MaterialError::dialogue_source)?
                                .unwrap_or(TaskProvenanceSourceKind::UserMessage),
                            content_ref,
                        });
                    }
                }
                DomainEvent::Conversation(ConversationEvent::MessageImported {
                    role,
                    content_ref,
                    ..
                }) => {
                    if role == MessageRole::User {
                        current_task_records.push(TaskProvenanceRecord {
                            record_id,
                            source_kind: TaskProvenanceSourceKind::ImportedUserMessage,
                            content_ref,
                        });
                    }
                }
                DomainEvent::Activity(ActivityEvent::DelegationContextProjected {
                    objective_ref,
                    ..
                }) => current_task_records.push(TaskProvenanceRecord {
                    record_id,
                    source_kind: TaskProvenanceSourceKind::DelegatedObjective,
                    content_ref: objective_ref,
                }),
                DomainEvent::Activity(ActivityEvent::StandingGoalContextProjected {
                    objective_ref,
                    input_refs,
                    ..
                }) => {
                    current_task_records.push(TaskProvenanceRecord {
                        record_id,
                        source_kind: TaskProvenanceSourceKind::StandingGoalObjective,
                        content_ref: objective_ref,
                    });
                    for reference in input_refs {
                        if seen.insert(reference.content_id) {
                            attachments.push(reference);
                        }
                    }
                }
                DomainEvent::Artifact(ArtifactEvent::VersionCommitted {
                    content_ref,
                    dependency_refs,
                    ..
                }) => {
                    for reference in std::iter::once(content_ref).chain(dependency_refs) {
                        if seen.insert(reference.content_id) {
                            attachments.push(reference);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(InitialActivityMaterial {
            attachments,
            current_task_records,
        })
    }
}

impl ToolExecutionMaterialSource for PostgresToolExecutionMaterialSource {
    fn load<'a>(
        &'a self,
        attempt: &'a AttemptContext,
        wire_name: &'a WireName,
    ) -> ApplicationFuture<'a, Result<ToolExecutionMaterial, ToolExecutionPortError>> {
        Box::pin(async move {
            self.load_inner(attempt, wire_name).await.map_err(|error| {
                tracing::warn!(
                    run_id = %attempt.run_id,
                    attempt_id = %attempt.attempt_id,
                    tool = %wire_name.as_str(),
                    material.error = error.kind(),
                    "Tool execution material reconstruction failed"
                );
                error.port()
            })
        })
    }
}

struct RunMaterialSource {
    owned_agent_id: Option<AgentId>,
    dialogue_input: Option<crate::dialogue_input_provenance::DialogueRunInput>,
    run_context: RunContext,
    actor: Actor,
    encoded_snapshot: Vec<u8>,
    execution_grant_proof: Option<EncodedExecutionGrantProof>,
    active_model_route_ordinal: u32,
}

/// Shared decoder for immutable prior-Run material. Skill discovery owns its
/// own query and state machine but must use the same typed RunContext mapping
/// as Tool execution rather than inventing a second wire interpretation.
pub(crate) fn decode_run_context_for_skill_discovery(
    tenant_id: TenantId,
    run_id: RunId,
    row: &sqlx::postgres::PgRow,
) -> Option<RunContext> {
    Some(RunContext {
        tenant_id,
        conversation_id: id(row.try_get("conversation_id").ok()?).ok()?,
        activity_id: id(row.try_get("activity_id").ok()?).ok()?,
        activity_kind: activity_kind(row.try_get("activity_kind").ok()?).ok()?,
        run_id,
        executor_participant_id: id(row.try_get("executor_participant_id").ok()?).ok()?,
        requested_by_participant_id: id(row.try_get("requested_by_participant_id").ok()?).ok()?,
        parent_run_id: optional_id(row.try_get("parent_run_id").ok()?).ok()?,
        delegation_id: optional_id(row.try_get("delegation_id").ok()?).ok()?,
        execution_scope: execution_scope(
            row.try_get("execution_scope_kind").ok()?,
            tenant_id,
            row.try_get("workspace_id").ok()?,
            row.try_get("device_id").ok()?,
        )
        .ok()?,
        control_home: runtime_home(
            row.try_get("control_home_kind").ok()?,
            row.try_get("control_device_id").ok()?,
        )
        .ok()?,
        execution_home: runtime_home(
            row.try_get("execution_home_kind").ok()?,
            row.try_get("execution_device_id").ok()?,
        )
        .ok()?,
        home_epoch: HomeEpoch::new(parse_u64(row.try_get("home_epoch").ok()?).ok()?).ok()?,
        runtime_snapshot_id: id(row.try_get("runtime_snapshot_id").ok()?).ok()?,
    })
}

struct RunLedgerMaterial {
    attachments: Vec<musteragent_content_ref::ContentRef>,
    prior_tool_results: Vec<musteragent_content_ref::ContentRef>,
    current_task_records: Vec<TaskProvenanceRecord>,
    exposure_state: ExposureState,
}

struct InitialActivityMaterial {
    attachments: Vec<musteragent_content_ref::ContentRef>,
    current_task_records: Vec<TaskProvenanceRecord>,
}

#[derive(Debug, thiserror::Error)]
enum MaterialError {
    #[error("dialogue input authority unavailable")]
    SourceAuthority,
    #[error("dialogue input custody unavailable")]
    SourceUnavailable,
    #[error("immutable application material unavailable")]
    ApplicationUnavailable,
    #[error("Tool material Attempt lease is stale")]
    StaleFence,
    #[error("Tool was not pinned into the active Runtime Snapshot")]
    ToolNotPinned,
    #[error("Tool was not exposed to the active Run")]
    ToolNotExposed,
    #[error("Tool material is corrupt")]
    Corrupt,
    #[error("Tool material database query failed")]
    Database(#[source] sqlx::Error),
}

impl From<RunLedgerReadError> for MaterialError {
    fn from(error: RunLedgerReadError) -> Self {
        match error {
            RunLedgerReadError::Corrupt => Self::Corrupt,
            RunLedgerReadError::Database(error) => Self::Database(error),
        }
    }
}

impl MaterialError {
    fn dialogue_source(error: musteragent_runtime_session::DialogueStoreError) -> Self {
        use musteragent_runtime_session::DialogueStoreError as E;
        match error {
            E::Infrastructure => Self::SourceUnavailable,
            E::Corrupt => Self::Corrupt,
            _ => Self::SourceAuthority,
        }
    }
    fn application(
        error: musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialError,
    ) -> Self {
        match error { musteragent_runtime_snapshot::application_catalog::ApplicationCatalogMaterialError::Unavailable => Self::ApplicationUnavailable, _ => Self::Corrupt }
    }
    const fn kind(&self) -> &'static str {
        match self {
            Self::SourceAuthority => "source_authority",
            Self::SourceUnavailable => "source_unavailable",
            Self::ApplicationUnavailable => "application_unavailable",
            Self::StaleFence => "stale_fence",
            Self::ToolNotPinned => "tool_not_pinned",
            Self::ToolNotExposed => "tool_not_exposed",
            Self::Corrupt => "corrupt",
            Self::Database(_) => "database",
        }
    }

    fn port(self) -> ToolExecutionPortError {
        match self {
            Self::SourceAuthority => ToolExecutionPortError {
                code: ErrorCode::PolicyDenied,
                retryable: false,
            },
            Self::StaleFence => ToolExecutionPortError {
                code: ErrorCode::AttemptLeaseStale,
                retryable: false,
            },
            Self::ToolNotPinned | Self::ToolNotExposed => ToolExecutionPortError {
                code: ErrorCode::InvalidCommand,
                retryable: false,
            },
            Self::Database(_) | Self::ApplicationUnavailable | Self::SourceUnavailable => {
                ToolExecutionPortError {
                    code: ErrorCode::DependencyUnavailable,
                    retryable: true,
                }
            }
            Self::Corrupt => ToolExecutionPortError {
                code: ErrorCode::InternalInvariantViolated,
                retryable: false,
            },
        }
    }
}

impl From<sqlx::Error> for MaterialError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value)
    }
}

fn activity_kind(value: i16) -> Result<ActivityKind, MaterialError> {
    match value {
        1 => Ok(ActivityKind::Chat),
        2 => Ok(ActivityKind::Coordination),
        3 => Ok(ActivityKind::Automation),
        4 => Ok(ActivityKind::Delegation),
        5 => Ok(ActivityKind::Maintenance),
        6 => Ok(ActivityKind::Evaluation),
        _ => Err(MaterialError::Corrupt),
    }
}

fn execution_scope(
    kind: i16,
    tenant_id: TenantId,
    workspace_id: Option<uuid::Uuid>,
    device_id: Option<uuid::Uuid>,
) -> Result<ExecutionScope, MaterialError> {
    match (kind, workspace_id, device_id) {
        (1, None, None) => Ok(ExecutionScope::Tenant { tenant_id }),
        (2, Some(workspace), None) => Ok(ExecutionScope::Workspace {
            workspace_id: id(workspace)?,
        }),
        (3, workspace, Some(device)) => Ok(ExecutionScope::DevicePrivate {
            device_id: id(device)?,
            workspace_id: workspace.map(id).transpose()?,
        }),
        _ => Err(MaterialError::Corrupt),
    }
}

fn runtime_home(kind: i16, device_id: Option<uuid::Uuid>) -> Result<RuntimeHome, MaterialError> {
    match (kind, device_id) {
        (1, None) => Ok(RuntimeHome::Cloud),
        (2, Some(device)) => Ok(RuntimeHome::Device {
            device_id: id(device)?,
        }),
        _ => Err(MaterialError::Corrupt),
    }
}

fn parse_u64(value: String) -> Result<u64, MaterialError> {
    value.parse().map_err(|_| MaterialError::Corrupt)
}

fn id<T: DatabaseId>(value: uuid::Uuid) -> Result<T, MaterialError> {
    T::from_database(value).map_err(|_| MaterialError::Corrupt)
}

fn optional_id<T: DatabaseId>(value: Option<uuid::Uuid>) -> Result<Option<T>, MaterialError> {
    value.map(id).transpose()
}

trait DatabaseId: Sized {
    fn from_database(value: uuid::Uuid) -> Result<Self, ()>;
}

macro_rules! database_ids {
    ($($type:ty),+ $(,)?) => {
        $(
            impl DatabaseId for $type {
                fn from_database(value: uuid::Uuid) -> Result<Self, ()> {
                    Self::from_uuid(value).map_err(|_| ())
                }
            }
        )+
    };
}

database_ids!(
    ActivityId,
    ConversationId,
    DelegationId,
    DeviceId,
    ParticipantId,
    PrincipalId,
    RunId,
    RuntimeSnapshotId,
    TenantId,
    WorkspaceId,
);
