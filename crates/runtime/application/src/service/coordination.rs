use std::collections::BTreeSet;

use musteragent_activity::{Activity, ActivityEvent, ActivityKind, ActivityStatus};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_conversation::{ConversationLifecycle, ParticipantKind, SubmitMessage};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_execution_grant::{GrantAuthorizationRequest, SignedExecutionGrant};
use musteragent_identity::{
    ActivityId, Actor, CommandId, ConversationId, ConversationSeq, DottedKey, IdempotencyKey,
    InterSessionMessageId, RunId, RunSeq, UtcTimestamp,
};
use musteragent_ledger::{
    AppendDisposition, AppendRequest, CoordinationAdmissionBatch, DomainEvent, ProposedOutbox,
    RuntimeSnapshotBlob,
};
use musteragent_policy::PermissionMode;
use musteragent_run::{Run, RunContext, RunEvent, RunState};
use musteragent_runtime_content::AdmissionContentSlot;
use musteragent_runtime_contract::{
    CommandEnvelope, CommandMetadata, ConversationGuard, ErrorCode, InputContent,
    RequestedFeatures, RuntimeError, TraceContext,
};
use musteragent_scheduler::{AdmissionSelection, AdmissionSurface};

use super::{RecordIdentity, RuntimeApplication, domain_record};
use crate::grant_binding::execution_grant_identity_binding;
use crate::identity::CoordinationIdentities;
use crate::runtime_error;
use crate::{
    AdmissionAuditContext, AdmissionAuthority, AdmissionAuthorizationRequest,
    CoordinationAdmissionCommit, SnapshotResolutionRequest,
};

const COMMAND_SCHEMA: &str = musteragent_runtime_contract::COMMAND_SCHEMA_VERSION;
const COORDINATION_TARGET_NONCE_DOMAIN_V1: &[u8] = b"musteragent.coordination-target-grant.v1\0";

/// Immutable target-side authority held across the Backend grant round trip.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CoordinationPreparation {
    actor: Actor,
    source_conversation_id: ConversationId,
    target_conversation_id: ConversationId,
    mailbox_message_id: InterSessionMessageId,
    deadline: UtcTimestamp,
    trace_context: TraceContext,
    execution_scope: ExecutionScope,
    control_home: RuntimeHome,
    authority: AdmissionAuthority,
    identities: CoordinationIdentities,
    already_admitted: bool,
}

impl CoordinationPreparation {
    /// Stable command identity to bind the target grant.
    #[must_use]
    pub const fn target_command_id(&self) -> CommandId {
        self.identities.command_id
    }

    /// Activity identity reserved for the target admission.
    #[must_use]
    pub const fn target_activity_id(&self) -> ActivityId {
        self.identities.admission.activity_id
    }

    /// Run identity reserved for the target admission.
    #[must_use]
    pub const fn target_run_id(&self) -> RunId {
        self.identities.admission.run_id
    }

    /// Authorized destination conversation.
    #[must_use]
    pub const fn target_conversation_id(&self) -> ConversationId {
        self.target_conversation_id
    }

    /// Authorized target execution scope.
    #[must_use]
    pub const fn execution_scope(&self) -> ExecutionScope {
        self.execution_scope
    }

    /// Whether the store already holds the original admission receipt.
    #[must_use]
    pub const fn already_admitted(&self) -> bool {
        self.already_admitted
    }

    /// Project the original identities as a duplicate receipt; call only after already_admitted.
    #[must_use]
    pub const fn replay_receipt(&self) -> CoordinationAdmissionReceipt {
        CoordinationAdmissionReceipt {
            command_id: self.identities.command_id,
            activity_id: self.identities.admission.activity_id,
            run_id: self.identities.admission.run_id,
            disposition: AppendDisposition::Duplicate,
        }
    }
}

/// Durable target identities and append disposition returned by coordination admission.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct CoordinationAdmissionReceipt {
    pub command_id: CommandId,
    pub activity_id: ActivityId,
    pub run_id: RunId,
    pub disposition: AppendDisposition,
}

impl RuntimeApplication {
    /// Authorize a distinct target conversation before obtaining its run-bound grant.
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_coordination_turn(
        &self,
        actor: Actor,
        source_conversation_id: ConversationId,
        target_conversation_id: ConversationId,
        mailbox_message_id: InterSessionMessageId,
        target_command_id: CommandId,
        deadline: UtcTimestamp,
        trace_context: TraceContext,
    ) -> Result<CoordinationPreparation, RuntimeError> {
        let identities = CoordinationIdentities::derive(target_command_id).ok_or_else(|| {
            coordination_error(
                ErrorCode::InternalInvariantViolated,
                musteragent_identity::CorrelationId::generate(),
                false,
                "Coordination identity derivation failed",
            )
        })?;
        if source_conversation_id == target_conversation_id {
            return Err(coordination_error(
                ErrorCode::InvalidCommand,
                identities.correlation_id,
                false,
                "Cross-Conversation delivery requires a distinct target Conversation",
            ));
        }
        let view = self
            .store
            .get_conversation(actor, target_conversation_id)
            .await?;
        if view.conversation.lifecycle != ConversationLifecycle::Active {
            return Err(coordination_error(
                ErrorCode::PolicyDenied,
                identities.correlation_id,
                false,
                "Queued turns require an active target Conversation",
            ));
        }
        let user_participant = view
            .participants
            .iter()
            .find(|participant| {
                participant.kind == ParticipantKind::User
                    && participant.principal_ref == Some(actor.principal_id)
                    && participant.left_at.is_none()
            })
            .ok_or_else(|| {
                coordination_error(
                    ErrorCode::PolicyDenied,
                    identities.correlation_id,
                    false,
                    "Actor is not an active participant of the target Conversation",
                )
            })?;
        let execution_scope = match view.conversation.control_home {
            RuntimeHome::Cloud => view.conversation.workspace_id.map_or(
                ExecutionScope::Tenant {
                    tenant_id: actor.tenant_id,
                },
                |workspace_id| ExecutionScope::Workspace { workspace_id },
            ),
            RuntimeHome::Device { device_id } => ExecutionScope::DevicePrivate {
                device_id,
                workspace_id: view.conversation.workspace_id,
            },
        };
        let already_admitted = match self.store.get_run(actor, identities.admission.run_id).await {
            Ok(run)
                if run.run.context.tenant_id == actor.tenant_id
                    && run.run.context.conversation_id == target_conversation_id
                    && run.run.context.activity_id == identities.admission.activity_id
                    && run.run.context.activity_kind == ActivityKind::Coordination =>
            {
                true
            }
            Ok(_) => {
                return Err(coordination_error(
                    ErrorCode::IdempotencyConflict,
                    identities.correlation_id,
                    false,
                    "Coordination identities conflict with an existing target Run",
                ));
            }
            Err(error) if error.code == ErrorCode::RunNotFound => false,
            Err(error) => return Err(error),
        };
        let now = self.clock.now();
        let authority = self
            .store
            .authorize_admission_for_home(AdmissionAuthorizationRequest {
                actor,
                conversation_id: target_conversation_id,
                participant_id: user_participant.participant_id,
                activity_kind: ActivityKind::Coordination,
                expected_home_epoch: view.conversation.home_epoch,
                expected_control_home: view.conversation.control_home,
                correlation_id: identities.correlation_id,
                audit_context: AdmissionAuditContext {
                    command_id: target_command_id,
                    trace_context: trace_context.clone(),
                    occurred_at: now,
                },
            })
            .await?;
        Ok(CoordinationPreparation {
            actor,
            source_conversation_id,
            target_conversation_id,
            mailbox_message_id,
            deadline,
            trace_context,
            execution_scope,
            control_home: view.conversation.control_home,
            authority,
            identities,
            already_admitted,
        })
    }

    /// Atomically admit the authorized mailbox turn with a fresh authenticated grant.
    pub async fn admit_coordination_turn(
        &self,
        preparation: CoordinationPreparation,
        payload: InputContent,
        signed_grant: SignedExecutionGrant,
    ) -> Result<CoordinationAdmissionReceipt, RuntimeError> {
        if preparation.already_admitted {
            return Ok(preparation.replay_receipt());
        }
        let correlation_id = preparation.identities.correlation_id;
        let now = self.clock.now();
        if now >= preparation.deadline {
            return Err(coordination_error(
                ErrorCode::DependencyTimeout,
                correlation_id,
                false,
                "Cross-Conversation turn deadline elapsed",
            ));
        }
        let execution_grant = self
            .grant_authenticator
            .authenticate(&signed_grant)
            .map_err(|code| {
                coordination_error(
                    code,
                    correlation_id,
                    false,
                    "Target Execution Grant authentication failed",
                )
            })?;
        let ids = preparation.identities.admission;
        if execution_grant.run_id != ids.run_id {
            return Err(coordination_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Target Execution Grant does not bind the queued Run",
            ));
        }
        if execution_grant.nonce.as_str()
            != coordination_target_grant_nonce(
                preparation.source_conversation_id,
                preparation.target_conversation_id,
                preparation.mailbox_message_id,
                preparation.identities.command_id,
                preparation.identities.admission.run_id,
            )
        {
            return Err(coordination_error(
                ErrorCode::PolicyDenied,
                correlation_id,
                false,
                "Target Execution Grant does not bind the mailbox provenance",
            ));
        }
        let command = coordination_command(&preparation, &execution_grant, payload.clone(), now)?;
        let run_context = RunContext {
            tenant_id: preparation.authority.tenant_id,
            conversation_id: preparation.target_conversation_id,
            activity_id: ids.activity_id,
            activity_kind: ActivityKind::Coordination,
            run_id: ids.run_id,
            executor_participant_id: preparation.authority.executor_participant_id,
            requested_by_participant_id: preparation.authority.requested_by_participant_id,
            parent_run_id: None,
            delegation_id: None,
            execution_scope: preparation.execution_scope,
            control_home: preparation.control_home,
            execution_home: preparation.control_home,
            home_epoch: preparation.authority.home_epoch,
            runtime_snapshot_id: ids.runtime_snapshot_id,
        };
        let assistant_identity = self
            .load_bound_assistant(
                preparation.actor,
                preparation.target_conversation_id,
                correlation_id,
            )
            .await?;
        let surface = execution_grant
            .surface
            .as_ref()
            .and_then(|value| AdmissionSurface::from_key(value.as_str()))
            .ok_or_else(|| {
                coordination_error(
                    ErrorCode::PolicyDenied,
                    correlation_id,
                    false,
                    "Target Execution Grant does not select a supported product surface",
                )
            })?;
        let resolved = self
            .snapshots
            .resolve(SnapshotResolutionRequest {
                actor: preparation.actor,
                command,
                run_context,
                now,
                assistant_identity,
                execution_grant: execution_grant.clone(),
                admission_selection: AdmissionSelection {
                    surface,
                    requested_skill_refs: BTreeSet::new(),
                },
                prior_discovery: preparation.authority.prior_discovery.clone(),
            })
            .await?;
        if resolved.snapshot.runtime_snapshot_id != ids.runtime_snapshot_id
            || resolved.snapshot.created_at != now
        {
            return Err(coordination_error(
                ErrorCode::InternalInvariantViolated,
                correlation_id,
                false,
                "Queued-turn Runtime Snapshot identity is inconsistent",
            ));
        }
        let identity_binding = execution_grant_identity_binding(
            preparation.authority.tenant_id,
            preparation.actor.principal_id,
            preparation.execution_scope,
        )
        .map_err(|_| {
            coordination_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Queued-turn identity binding is invalid",
            )
        })?;
        execution_grant
            .authorize(&GrantAuthorizationRequest {
                runtime_audience: self.home_policy.runtime_audience().clone(),
                tenant_id: preparation.authority.tenant_id,
                identity_binding,
                run_id: ids.run_id,
                model_provider_id: resolved.snapshot.content.model.route.model_provider_id,
                model_entry_id: resolved.snapshot.content.model.route.model_entry_id,
                now,
            })
            .map_err(|_| {
                coordination_error(
                    ErrorCode::PolicyDenied,
                    correlation_id,
                    false,
                    "Target Execution Grant is not authorized for the queued Run",
                )
            })?;
        let fingerprint = coordination_fingerprint(&preparation, &payload);
        let content_stage = self
            .prepare_admission_content(
                preparation.authority.tenant_id,
                preparation.identities.command_id,
                fingerprint,
                AdmissionContentSlot::CoordinationContext,
                &payload,
                correlation_id,
            )
            .await?;
        let batch = coordination_batch(
            self,
            &preparation,
            now,
            fingerprint,
            content_stage.content.clone(),
            resolved,
            execution_grant,
            self.execution_grant_proof(&signed_grant, correlation_id)?,
        )?;
        let receipt = self
            .store
            .admit_coordination_content(
                correlation_id,
                CoordinationAdmissionCommit {
                    authority: preparation.authority,
                    command_id: preparation.identities.command_id,
                    mailbox_message_id: preparation.mailbox_message_id,
                    batch,
                    content_stage,
                },
            )
            .await?;
        Ok(CoordinationAdmissionReceipt {
            command_id: preparation.identities.command_id,
            activity_id: ids.activity_id,
            run_id: ids.run_id,
            disposition: receipt.disposition,
        })
    }
}

/// Derives the non-secret, deterministic nonce that binds a target-side
/// Execution Grant to one exact durable mailbox delivery. Grant issuers and
/// Runtime admission use this same canonical byte layout; callers must not
/// recreate it from display text or mutable delivery state.
#[must_use]
pub fn coordination_target_grant_nonce(
    source_conversation_id: ConversationId,
    target_conversation_id: ConversationId,
    mailbox_message_id: InterSessionMessageId,
    target_command_id: CommandId,
    target_run_id: RunId,
) -> String {
    let mut material = Vec::with_capacity(COORDINATION_TARGET_NONCE_DOMAIN_V1.len() + 80);
    material.extend_from_slice(COORDINATION_TARGET_NONCE_DOMAIN_V1);
    material.extend_from_slice(source_conversation_id.into_uuid().as_bytes());
    material.extend_from_slice(target_conversation_id.into_uuid().as_bytes());
    material.extend_from_slice(mailbox_message_id.into_uuid().as_bytes());
    material.extend_from_slice(target_command_id.into_uuid().as_bytes());
    material.extend_from_slice(target_run_id.into_uuid().as_bytes());
    format!("coordination_{}", Sha256::digest(&material))
}

fn coordination_command(
    preparation: &CoordinationPreparation,
    grant: &musteragent_execution_grant::ExecutionGrant,
    payload: InputContent,
    now: UtcTimestamp,
) -> Result<CommandEnvelope<SubmitMessage>, RuntimeError> {
    let metadata = CommandMetadata {
        schema_version: DottedKey::new(COMMAND_SCHEMA).map_err(|_| {
            coordination_error(
                ErrorCode::InternalInvariantViolated,
                preparation.identities.correlation_id,
                false,
                "Coordination command schema is invalid",
            )
        })?,
        command_id: preparation.identities.command_id,
        actor: preparation.actor,
        idempotency_key: IdempotencyKey::new(format!(
            "coordination-turn:{}",
            preparation.mailbox_message_id
        ))
        .map_err(|_| {
            coordination_error(
                ErrorCode::InternalInvariantViolated,
                preparation.identities.correlation_id,
                false,
                "Coordination idempotency identity is invalid",
            )
        })?,
        issued_at: now,
        deadline: preparation.deadline,
        correlation_id: preparation.identities.correlation_id,
        causation_id: None,
        conversation_guard: Some(ConversationGuard {
            conversation_id: preparation.target_conversation_id,
            expected_home_epoch: preparation.authority.home_epoch,
            expected_conversation_seq: ConversationSeq::new(
                preparation.authority.current_offset.get(),
            )
            .ok(),
        }),
        trace_context: preparation.trace_context.clone(),
    };
    CommandEnvelope::new(
        metadata,
        SubmitMessage {
            project_task_target: None,
            message_id: preparation.identities.synthetic_message_id,
            participant_id: preparation.authority.requested_by_participant_id,
            content: payload,
            attachment_refs: Vec::new(),
            requested_model_class: grant.requested_model_class.clone(),
            requested_features: RequestedFeatures {
                permission_mode: grant.permission_mode.unwrap_or(PermissionMode::Supervised),
                ..RequestedFeatures::default()
            },
            execution_scope: preparation.execution_scope,
        },
    )
    .map_err(|_| {
        coordination_error(
            ErrorCode::InvalidCommand,
            preparation.identities.correlation_id,
            false,
            "Coordination command is invalid",
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn coordination_batch(
    application: &RuntimeApplication,
    preparation: &CoordinationPreparation,
    now: UtcTimestamp,
    fingerprint: Sha256,
    payload_ref: ContentRef,
    resolved: crate::RuntimeSnapshotResolution,
    execution_grant: musteragent_execution_grant::ExecutionGrant,
    execution_grant_proof: Option<musteragent_execution_grant::EncodedExecutionGrantProof>,
) -> Result<CoordinationAdmissionBatch, RuntimeError> {
    let ids = preparation.identities.admission;
    let activity = Activity {
        activity_id: ids.activity_id,
        conversation_id: preparation.target_conversation_id,
        kind: ActivityKind::Coordination,
        requested_by_participant_id: preparation.authority.requested_by_participant_id,
        parent_activity_id: None,
        root_activity_id: ids.activity_id,
        status: ActivityStatus::Active,
        policy_snapshot_ref: resolved.snapshot.content.policy.policy_ref.clone(),
        created_at: now,
        completed_at: None,
    };
    let run_context = RunContext {
        tenant_id: preparation.authority.tenant_id,
        conversation_id: preparation.target_conversation_id,
        activity_id: ids.activity_id,
        activity_kind: ActivityKind::Coordination,
        run_id: ids.run_id,
        executor_participant_id: preparation.authority.executor_participant_id,
        requested_by_participant_id: preparation.authority.requested_by_participant_id,
        parent_run_id: None,
        delegation_id: None,
        execution_scope: preparation.execution_scope,
        control_home: preparation.control_home,
        execution_home: preparation.control_home,
        home_epoch: preparation.authority.home_epoch,
        runtime_snapshot_id: ids.runtime_snapshot_id,
    };
    let run = Run {
        context: run_context,
        state: RunState::queued(),
        next_attempt_number: musteragent_identity::AttemptNumber::new(1).map_err(|_| {
            coordination_error(
                ErrorCode::InternalInvariantViolated,
                preparation.identities.correlation_id,
                false,
                "Coordination attempt identity is invalid",
            )
        })?,
        not_before: None,
        cancel_requested_at: None,
        admitted_at: now,
        started_at: None,
        finished_at: None,
        terminal_error: None,
    };
    let records = vec![
        domain_record(
            application.encoder.as_ref(),
            RecordIdentity {
                record_id: ids.message_record_id,
                activity_id: Some(ids.activity_id),
                run_id: None,
                run_seq: None,
                participant_id: Some(preparation.authority.requested_by_participant_id),
            },
            DomainEvent::Activity(ActivityEvent::CoordinationContextProjected {
                mailbox_message_id: preparation.mailbox_message_id,
                source_conversation_id: preparation.source_conversation_id,
                payload_ref: payload_ref.clone(),
            }),
            now,
            preparation.identities.correlation_id,
        )
        .map_err(|code| {
            coordination_error(
                code,
                preparation.identities.correlation_id,
                false,
                "Coordination context record is invalid",
            )
        })?,
        domain_record(
            application.encoder.as_ref(),
            RecordIdentity {
                record_id: ids.activity_record_id,
                activity_id: Some(ids.activity_id),
                run_id: None,
                run_seq: None,
                participant_id: Some(preparation.authority.requested_by_participant_id),
            },
            DomainEvent::Activity(ActivityEvent::Started {
                kind: ActivityKind::Coordination,
                root_activity_id: ids.activity_id,
                parent_activity_id: None,
                policy_snapshot_ref: activity.policy_snapshot_ref.clone(),
            }),
            now,
            preparation.identities.correlation_id,
        )
        .map_err(|code| {
            coordination_error(
                code,
                preparation.identities.correlation_id,
                false,
                "Coordination Activity record is invalid",
            )
        })?,
        domain_record(
            application.encoder.as_ref(),
            RecordIdentity {
                record_id: ids.run_record_id,
                activity_id: Some(ids.activity_id),
                run_id: Some(ids.run_id),
                run_seq: Some(RunSeq::new(1).map_err(|_| {
                    coordination_error(
                        ErrorCode::InternalInvariantViolated,
                        preparation.identities.correlation_id,
                        false,
                        "Coordination Run sequence is invalid",
                    )
                })?),
                participant_id: Some(preparation.authority.executor_participant_id),
            },
            DomainEvent::Run(RunEvent::Admitted {
                runtime_snapshot_id: ids.runtime_snapshot_id,
                execution_home: preparation.control_home,
                parent_run_id: None,
                delegation_id: None,
                input_message_id: None,
                retry_source_message_id: None,
                retry_source_attempt_number: None,
            }),
            now,
            preparation.identities.correlation_id,
        )
        .map_err(|code| {
            coordination_error(
                code,
                preparation.identities.correlation_id,
                false,
                "Coordination Run record is invalid",
            )
        })?,
    ];
    let dispatch = format!("run:{}", ids.run_id).into_bytes();
    let append = AppendRequest {
        tenant_id: preparation.authority.tenant_id,
        conversation_id: preparation.target_conversation_id,
        expected_home_epoch: preparation.authority.home_epoch,
        expected_offset: preparation.authority.current_offset,
        idempotency_key: IdempotencyKey::new(format!(
            "coordination-turn:{}",
            preparation.mailbox_message_id
        ))
        .map_err(|_| {
            coordination_error(
                ErrorCode::InternalInvariantViolated,
                preparation.identities.correlation_id,
                false,
                "Coordination append idempotency identity is invalid",
            )
        })?,
        command_fingerprint: fingerprint,
        records,
        outbox: vec![ProposedOutbox {
            message_id: ids.dispatch_id,
            topic: DottedKey::new("run.dispatch").map_err(|_| {
                coordination_error(
                    ErrorCode::InternalInvariantViolated,
                    preparation.identities.correlation_id,
                    false,
                    "Coordination dispatch topic is invalid",
                )
            })?,
            payload_hash: Sha256::digest(&dispatch),
            payload: dispatch,
        }],
    };
    let memory_selection = resolved.snapshot.content.memory.clone();
    let instinct_selections = resolved.snapshot.content.instincts.clone();
    let memory_agent_id = resolved.snapshot.content.agent.agent_id;
    Ok(CoordinationAdmissionBatch {
        actor: preparation.actor,
        trace_context: preparation.trace_context.clone(),
        append,
        context_record_id: ids.message_record_id,
        activity_record_id: ids.activity_record_id,
        run_record_id: ids.run_record_id,
        mailbox_message_id: preparation.mailbox_message_id,
        source_conversation_id: preparation.source_conversation_id,
        payload_ref,
        activity,
        run,
        runtime_snapshot: RuntimeSnapshotBlob {
            runtime_snapshot_id: ids.runtime_snapshot_id,
            schema_version: resolved.snapshot.content.schema_version.clone(),
            encoded_snapshot: resolved.snapshot.encoded_content,
            content_hash: resolved.snapshot.content_hash,
            created_at: resolved.snapshot.created_at,
            memory_selection,
            instinct_selections,
            memory_agent_id,
            execution_grant,
            execution_grant_proof,
        },
    })
}

fn coordination_fingerprint(
    preparation: &CoordinationPreparation,
    payload: &InputContent,
) -> Sha256 {
    let mut bytes = b"musteragent.coordination-admission.v1\0".to_vec();
    bytes.extend_from_slice(preparation.actor.tenant_id.into_uuid().as_bytes());
    bytes.extend_from_slice(preparation.actor.principal_id.into_uuid().as_bytes());
    bytes.extend_from_slice(preparation.source_conversation_id.into_uuid().as_bytes());
    bytes.extend_from_slice(preparation.target_conversation_id.into_uuid().as_bytes());
    bytes.extend_from_slice(preparation.mailbox_message_id.into_uuid().as_bytes());
    bytes.extend_from_slice(
        preparation
            .identities
            .admission
            .run_id
            .into_uuid()
            .as_bytes(),
    );
    match payload {
        InputContent::InlineUtf8(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&(value.as_str().len() as u64).to_be_bytes());
            bytes.extend_from_slice(value.as_str().as_bytes());
        }
        InputContent::ContentRef(reference) => {
            bytes.push(2);
            bytes.extend_from_slice(reference.content_id.into_uuid().as_bytes());
            bytes.extend_from_slice(&reference.byte_length.to_be_bytes());
            bytes.extend_from_slice(&reference.sha256.into_bytes());
            bytes.extend_from_slice(reference.media_type.as_str().as_bytes());
        }
    }
    Sha256::digest(bytes)
}

fn coordination_error(
    code: ErrorCode,
    correlation_id: musteragent_identity::CorrelationId,
    retryable: bool,
    message: &'static str,
) -> RuntimeError {
    runtime_error(code, correlation_id, retryable, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_nonce_matches_the_backend_known_answer() {
        assert_eq!(
            coordination_target_grant_nonce(
                "019fa6de-38fd-7000-8000-000000000610"
                    .parse()
                    .expect("source Conversation"),
                "019fa6de-38fd-7000-8000-000000000615"
                    .parse()
                    .expect("target Conversation"),
                "019fa6de-38fd-7000-8000-000000000616"
                    .parse()
                    .expect("mailbox message"),
                "019fa6de-38fd-7000-8000-000000000617"
                    .parse()
                    .expect("target Command"),
                "2026abfa-b41e-55ba-8fb5-fa017763c0aa"
                    .parse()
                    .expect("target Run"),
            ),
            "coordination_313fb5e79c00fe91b63f6a9a6b96908eed6946250f3cfd9bc41616345f7f4f5a",
        );
    }
}
