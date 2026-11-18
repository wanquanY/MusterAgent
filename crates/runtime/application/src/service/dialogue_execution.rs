//! Converts verified Dialogue owner facts into the ordinary Runtime admission
//! batch. This does not accept public Workbench submission authority.
use super::*;
use crate::{AdmissionAuthority, DialogueExecutionAdmissionCommit, DialogueExecutionPreparation};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_identity::{CorrelationId, IdempotencyKey};
use musteragent_runtime_contract::{
    COMMAND_SCHEMA_VERSION, CommandMetadata, ConversationGuard, InputContent, RequestedFeatures,
    TraceContext,
};

impl RuntimeApplication {
    /// Prepare the atomic create-and-submit commit for a frozen private input.
    /// The DialogueExecutionStore verifies the source again inside its transaction.
    pub async fn prepare_dialogue_admission(
        &self,
        preparation: DialogueExecutionPreparation,
        selection: AdmissionSelection,
        signed_grant: SignedExecutionGrant,
        deadline: UtcTimestamp,
        trace_context: TraceContext,
    ) -> Result<DialogueExecutionAdmissionCommit, RuntimeError> {
        let correlation = CorrelationId::from_uuid(preparation.identities.command_id.into_uuid())
            .expect("nonzero command id");
        let fail = |code| {
            runtime_error(
                code,
                correlation,
                false,
                "Dialogue execution admission is not authorized",
            )
        };
        preparation
            .validate()
            .map_err(|_| fail(ErrorCode::InvalidCommand))?;
        let now = self.clock.now();
        let grant = self
            .grant_authenticator
            .authenticate(&signed_grant)
            .map_err(fail)?;
        if self.home_policy.control_home() != RuntimeHome::Cloud
            || self.home_policy.execution_home() != RuntimeHome::Cloud
            || grant.permission_mode != Some(musteragent_policy::PermissionMode::ServiceManaged)
        {
            return Err(fail(ErrorCode::PolicyDenied));
        }
        let scope = ExecutionScope::Tenant {
            tenant_id: preparation.scope.tenant_id,
        };
        self.home_policy
            .validate_execution_scope(preparation.scope.tenant_id, scope)
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
        let permission_mode = grant
            .permission_mode
            .ok_or_else(|| fail(ErrorCode::PolicyDenied))?;
        let identities = preparation.identities;
        let ids = AdmissionIdentities::derive(identities.command_id)
            .ok_or_else(|| fail(ErrorCode::InternalInvariantViolated))?;
        let home_epoch = HomeEpoch::new(1).expect("constant");
        let command = CommandEnvelope::new(
            CommandMetadata {
                schema_version: DottedKey::new(COMMAND_SCHEMA_VERSION)
                    .map_err(|_| fail(ErrorCode::InvalidCommand))?,
                command_id: identities.command_id,
                actor: preparation.actor(),
                idempotency_key: IdempotencyKey::new(format!(
                    "dialogue-intent:{}",
                    preparation.scope.source_intent_id
                ))
                .map_err(|_| fail(ErrorCode::InvalidCommand))?,
                issued_at: now,
                deadline,
                correlation_id: correlation,
                causation_id: None,
                conversation_guard: Some(ConversationGuard {
                    conversation_id: identities.conversation_id,
                    expected_home_epoch: home_epoch,
                    expected_conversation_seq: None,
                }),
                trace_context,
            },
            SubmitMessage {
                project_task_target: None,
                message_id: identities.message_id,
                participant_id: identities.requested_by,
                content: InputContent::ContentRef(preparation.intent.content_ref.clone()),
                attachment_refs: preparation
                    .intent
                    .input_attachments
                    .iter()
                    .map(|value| value.attachment.clone())
                    .collect(),
                requested_model_class: grant.requested_model_class.clone(),
                requested_features: RequestedFeatures {
                    permission_mode,
                    ..RequestedFeatures::default()
                },
                execution_scope: scope,
            },
        )
        .map_err(|_| fail(ErrorCode::InvalidCommand))?;
        let authority = AdmissionAuthority {
            tenant_id: preparation.scope.tenant_id,
            conversation_id: identities.conversation_id,
            home_epoch,
            current_offset: LedgerOffset::ZERO,
            title: None,
            has_user_messages: true,
            active_context_branch_id: None,
            prior_discovery: None,
            requested_by_participant_id: identities.requested_by,
            executor_participant_id: identities.executor,
        };
        let run_context =
            resolved_request_run_context(&self.home_policy, &authority, &command, &ids);
        let request = SnapshotResolutionRequest {
            actor: preparation.actor(),
            command: command.clone(),
            run_context,
            now,
            assistant_identity: preparation.assistant_identity.clone(),
            execution_grant: grant.clone(),
            admission_selection: selection.clone(),
            prior_discovery: None,
        };
        let resolved = self
            .snapshots
            .resolve_dialogue(request, preparation.clone())
            .await
            .inspect_err(|error| {
                tracing::warn!(
                    correlation_id = correlation.to_string().as_str(),
                    run_id = ids.run_id.to_string().as_str(),
                    failure.stage = "snapshot_resolution",
                    error.code = error.code.as_str(),
                    "Dialogue Snapshot resolution failed"
                );
            })?;
        // The product release's persona/input-consumption gate is host-owned.
        // The neutral source commitment and sealed Tool contracts are kernel authority.
        crate::validate_dialogue_snapshot_source(&resolved.snapshot.content, &preparation)
            .map_err(fail)?;
        if resolved.snapshot.runtime_snapshot_id != ids.runtime_snapshot_id
            || resolved.snapshot.created_at != now
        {
            return Err(fail(ErrorCode::RuntimeSnapshotInvalid));
        }
        grant
            .authorize(&GrantAuthorizationRequest {
                runtime_audience: self.home_policy.runtime_audience().clone(),
                tenant_id: preparation.scope.tenant_id,
                identity_binding: execution_grant_identity_binding(
                    preparation.scope.tenant_id,
                    preparation.scope.owner_id,
                    scope,
                )
                .map_err(|_| fail(ErrorCode::PolicyDenied))?,
                run_id: ids.run_id,
                model_provider_id: resolved.snapshot.content.model.route.model_provider_id,
                model_entry_id: resolved.snapshot.content.model.route.model_entry_id,
                now,
            })
            .map_err(|_| fail(ErrorCode::PolicyDenied))?;
        let command_fingerprint = submit_command_fingerprint(&command, &selection);
        let content_stage = self
            .prepare_admission_content(
                preparation.scope.tenant_id,
                identities.command_id,
                command_fingerprint,
                AdmissionContentSlot::SubmitMessage,
                &command.payload.content,
                correlation,
            )
            .await
            .inspect_err(|error| {
                tracing::warn!(
                    correlation_id = correlation.to_string().as_str(),
                    run_id = ids.run_id.to_string().as_str(),
                    failure.stage = "admission_content_staging",
                    error.code = error.code.as_str(),
                    "Dialogue input material staging failed"
                );
            })?;
        let proof = self.execution_grant_proof(&signed_grant, correlation)?;
        let batch = self
            .build_admission_batch(
                command,
                authority,
                LedgerOffset::ZERO,
                content_stage.content.clone(),
                AdmissionPreparation {
                    ids,
                    now,
                    resolved,
                    execution_grant: grant,
                    execution_grant_proof: proof,
                },
                (
                    command_fingerprint,
                    &selection,
                    self.home_policy.execution_home(),
                ),
            )
            .map_err(fail)?;
        let commit = DialogueExecutionAdmissionCommit {
            preparation,
            admission: AdmissionCommit {
                batch,
                content_stage,
            },
        };
        commit
            .validate()
            .map_err(|_| fail(ErrorCode::RuntimeSnapshotInvalid))?;
        Ok(commit)
    }
}
