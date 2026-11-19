#![allow(
    dead_code,
    reason = "Shared fixture used across admission and projection conformance binaries."
)]

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use musteragent_capability::{CapabilityEntitlement, CapabilityKey, CapabilityLocation};
use musteragent_content_ref::{ContentRef, MediaType, SchemaRef, Sha256};
use musteragent_conversation::{
    ConversationApplication, ConversationKind, CreateConversation, GetConversation,
    ParticipantKind, RetractMessage, SubmitMessage,
};
use musteragent_definition::{AgentDefinitionStore, PublishedAgentVersionPointer};
use musteragent_event_envelope::{ExecutionScope, RuntimeHome};
use musteragent_execution_grant::{
    EXECUTION_GRANT_SCHEMA_V10, ExecutionGrant, ExecutionGrantAuthorityScope,
    ExecutionGrantIdentityBinding, GrantedAuxiliaryModelRoute, SignedExecutionGrant,
};
use musteragent_execution_liveness_io::SystemLivenessHomeClock;
use musteragent_identity::{
    Actor, AgentId, AgentKey, AttemptNumber, CapabilityProviderId, CommandId, ConversationId,
    ConversationSeq, CorrelationId, DottedKey, IdempotencyKey, InterSessionMessageId, InvocationId,
    MessageId, ParticipantId, PrincipalId, PrincipalKind, ProductId, RunId, TenantId, ToolCallId,
    UtcTimestamp, VersionedKey,
};
use musteragent_ledger::{DomainEvent, LedgerPayload, LedgerPayloadEncoder};
use musteragent_loop::AgentLoopJournal;
use musteragent_model::{
    ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_policy::{ApprovalRequirement, Decision, Effect, PermissionMode};
use musteragent_postgres::{
    PostgresAgentDefinitionStore, PostgresAgentLoopJournal, PostgresCloudDelegation,
    PostgresContentStore, PostgresConversationProjection, PostgresDispatchStore,
    PostgresInteractionApplicationStore, PostgresRuntimeApplicationStore,
    PostgresSecurityAuditPipeline, PostgresToolInvocationStore, migrate,
};
use musteragent_projection::{
    ConversationProjectionApplication, GetConversationProjection, SubscriptionError,
    SubscriptionErrorMapper,
};
use musteragent_run::{
    AttemptContext, CancelRun, EnqueueRunControl, GetRun, RetryRun, RunApplication, RunControlKind,
    RunEvent, RunStatus, WorkerId,
};
use musteragent_runtime_application::{
    ApplicationClock, KernelSnapshotBuilder, RuntimeApplication, RuntimeApplicationPorts,
    RuntimeSnapshotResolution, RuntimeSnapshotResolver, SnapshotResolutionRequest,
    coordination_target_grant_nonce, derive_admission_run_id, runtime_error,
};
use musteragent_runtime_automation::CloudDelegationTools;
use musteragent_runtime_capability::{
    DialogueToolInvocation, ToolInvocationStore, ToolPersistenceDisposition,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    ApplicationFuture, CancelReason, CommandDisposition, CommandEnvelope, CommandMetadata,
    ConversationGuard, DisplayText, ErrorCode, ErrorDetails, InlineInput, InputContent, ModelClass,
    OpaqueExternalRef, RequestedFeatures, RuntimeError, SafeDisplayText, TraceContext, TraceParent,
    execution_liveness::InvocationExecutionTiming,
};
use musteragent_runtime_snapshot::{RunExecutionPolicySnapshot, RuntimeSnapshot};
use musteragent_scheduler::{
    AdmissionApplication, AdmissionSelection, ClaimOutcome, DispatchStore,
    ExecutionGrantAuthenticator,
};
use musteragent_skill::builtin_skill_catalog_entries;
use musteragent_storage_protection::{OpaqueRefKeyring, ProtectedContentKeyring};
use musteragent_tool::{
    BuiltinToolCatalog, CapabilityExecutionTarget, ToolInvocation, ToolInvocationStatus, ToolKey,
    ToolRetryContract,
};
use musteragent_wire::{
    ProtobufConversationProjectionCodec, ProtobufExecutionGrantProofEncoder,
    ProtobufLedgerPayloadCodec, ProtobufMeteringPublicationEncoder, ProtobufSchedulerEventEncoder,
    ProtobufSnapshotCodec,
};
use sqlx::PgPool;

#[derive(Debug)]
pub struct MovingClock(std::sync::atomic::AtomicI64);

impl MovingClock {
    pub fn new(start: UtcTimestamp) -> Self {
        Self(std::sync::atomic::AtomicI64::new(start.unix_millis()))
    }
}

impl ApplicationClock for MovingClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::from_unix_millis(self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TestSubscriptionErrorMapper;

impl SubscriptionErrorMapper for TestSubscriptionErrorMapper {
    fn map(&self, _error: SubscriptionError) -> RuntimeError {
        let mut error = runtime_error(
            ErrorCode::InternalInvariantViolated,
            CorrelationId::generate(),
            false,
            "projection test error",
        );
        error.details = Box::new(ErrorDetails::None);
        error
    }
}

#[derive(Debug, Copy, Clone)]
struct TestGrantAuthenticator;

impl ExecutionGrantAuthenticator for TestGrantAuthenticator {
    fn authenticate(&self, signed: &SignedExecutionGrant) -> Result<ExecutionGrant, ErrorCode> {
        Ok(signed.claims.clone())
    }
}

struct TestSnapshotResolver {
    snapshots: KernelSnapshotBuilder,
}

impl TestSnapshotResolver {
    fn production(content: Arc<dyn ContentStore>) -> Self {
        Self {
            snapshots: KernelSnapshotBuilder::new(
                content,
                RunExecutionPolicySnapshot::production_defaults(),
            ),
        }
    }
}

impl RuntimeSnapshotResolver for TestSnapshotResolver {
    fn resolve(
        &self,
        request: SnapshotResolutionRequest,
    ) -> ApplicationFuture<'_, Result<RuntimeSnapshotResolution, RuntimeError>> {
        Box::pin(async move {
            let content = self
                .snapshots
                .build_content(&request)
                .await
                .map_err(|code| {
                    runtime_error(
                        code,
                        request.command.metadata.correlation_id,
                        code == ErrorCode::DependencyUnavailable,
                        "Host snapshot material could not be built",
                    )
                })?;
            RuntimeSnapshot::seal(
                request.run_context.runtime_snapshot_id,
                content,
                request.now,
                &ProtobufSnapshotCodec,
            )
            .map(|snapshot| RuntimeSnapshotResolution { snapshot })
            .map_err(|_| {
                runtime_error(
                    ErrorCode::RuntimeSnapshotInvalid,
                    request.command.metadata.correlation_id,
                    false,
                    "Invalid host snapshot",
                )
            })
        })
    }

    fn reseal_retry(
        &self,
        _tenant: TenantId,
        id: musteragent_identity::RuntimeSnapshotId,
        source: musteragent_runtime_snapshot::RuntimeSnapshotContent,
        now: UtcTimestamp,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<RuntimeSnapshotResolution, RuntimeError>> {
        Box::pin(async move {
            RuntimeSnapshot::seal(id, source, now, &ProtobufSnapshotCodec)
                .map(|snapshot| RuntimeSnapshotResolution { snapshot })
                .map_err(|_| {
                    runtime_error(
                        ErrorCode::RuntimeSnapshotInvalid,
                        correlation,
                        false,
                        "Invalid frozen retry snapshot",
                    )
                })
        })
    }
}

/// Encodes `RunEvent::Admitted` without `input_message_id` (simulating pre-K3 historical records).
#[derive(Debug, Default, Copy, Clone)]
pub struct LegacyPreK3LedgerEncoder;

impl LedgerPayloadEncoder for LegacyPreK3LedgerEncoder {
    fn encode(&self, payload: LedgerPayload) -> Vec<u8> {
        let stripped = match payload {
            LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Admitted {
                runtime_snapshot_id,
                execution_home,
                parent_run_id,
                delegation_id,
                input_message_id: _,
                retry_source_message_id,
                retry_source_attempt_number,
            })) => LedgerPayload::DomainEvent(DomainEvent::Run(RunEvent::Admitted {
                runtime_snapshot_id,
                execution_home,
                parent_run_id,
                delegation_id,
                input_message_id: None,
                retry_source_message_id,
                retry_source_attempt_number,
            })),
            other => other,
        };
        ProtobufLedgerPayloadCodec.encode(stripped)
    }
}

pub struct InputMessageHarness {
    pub pool: PgPool,
    pub actor: Actor,
    pub now: UtcTimestamp,
    pub keys: Arc<OpaqueRefKeyring>,
    pub recovery_keys: Arc<ProtectedContentKeyring>,
    pub content_store: PostgresContentStore,
    pub content: Arc<dyn ContentStore>,
    pub application: RuntimeApplication,
    pub projection: PostgresConversationProjection,
}

impl InputMessageHarness {
    pub async fn new(pool: PgPool) -> Self {
        Self::with_encoder(pool, Arc::new(ProtobufLedgerPayloadCodec)).await
    }

    pub async fn with_legacy_encoder(pool: PgPool) -> Self {
        Self::with_encoder(pool, Arc::new(LegacyPreK3LedgerEncoder)).await
    }

    async fn with_encoder(pool: PgPool, encoder: Arc<dyn LedgerPayloadEncoder>) -> Self {
        migrate(&pool).await.expect("apply migrations");
        let key_id = DottedKey::new("input.message.conformance").expect("key id");
        let keys = Arc::new(
            OpaqueRefKeyring::new(key_id.clone(), [(key_id.clone(), [43_u8; 32])])
                .expect("reference keyring"),
        );
        let recovery_keys = Arc::new(
            ProtectedContentKeyring::new(key_id.clone(), [(key_id, [43_u8; 32])])
                .expect("recovery keyring"),
        );
        let content_store = PostgresContentStore::new(
            pool.clone(),
            Arc::new(ObjectStoreBlobStore::in_memory()),
            Arc::clone(&keys),
        );
        for entry in builtin_skill_catalog_entries().expect("builtin skill catalog") {
            let reference = entry.definition.instructions_ref;
            let bytes = musteragent_skill::builtin_skill_instructions(&reference)
                .expect("compiled skill instructions");
            content_store
                .retain_runtime_release_at(&reference, bytes.as_bytes().to_vec())
                .await
                .expect("retain builtin skill instructions");
        }
        let content: Arc<dyn ContentStore> = Arc::new(content_store.clone());
        let actor = Actor {
            tenant_id: TenantId::generate(),
            principal_id: PrincipalId::generate(),
            principal_kind: PrincipalKind::User,
        };
        let now_ms: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
        )
        .fetch_one(&pool)
        .await
        .expect("database timestamp");
        let now = UtcTimestamp::from_unix_millis(now_ms);
        publish_host_agent(&pool, actor, content.as_ref()).await;

        let audit = Arc::new(PostgresSecurityAuditPipeline::standard(pool.clone()));
        let store = Arc::new(
            PostgresRuntimeApplicationStore::with_security_audit(
                pool.clone(),
                Arc::new(ProtobufSnapshotCodec),
                Arc::clone(&audit),
            )
            .with_admission_content_store(content_store.clone()),
        );
        let interactions = Arc::new(PostgresInteractionApplicationStore::with_security_audit(
            pool.clone(),
            Arc::clone(&keys),
            Arc::new(ProtobufLedgerPayloadCodec),
            audit,
        ));
        let application = RuntimeApplication::new(
            AgentKey::new("assistant").expect("agent key"),
            RuntimeApplicationPorts {
                store,
                snapshots: Arc::new(TestSnapshotResolver::production(Arc::clone(&content))),
                content: Arc::clone(&content),
                encoder,
                clock: Arc::new(MovingClock::new(now)),
                grant_authenticator: Arc::new(TestGrantAuthenticator),
                interactions,
            },
        )
        .with_execution_grant_proof_encoder(Arc::new(ProtobufExecutionGrantProofEncoder));
        let projection = PostgresConversationProjection::new(
            pool.clone(),
            Arc::new(ProtobufLedgerPayloadCodec),
            Arc::new(ProtobufConversationProjectionCodec),
            Arc::clone(&content),
            Arc::new(TestSubscriptionErrorMapper),
        );
        Self {
            pool,
            actor,
            now,
            keys,
            recovery_keys,
            content_store,
            content,
            application,
            projection,
        }
    }

    pub async fn create_conversation(&self, tag: &str) -> (ConversationId, ParticipantId) {
        let conversation_id = ConversationId::generate();
        let command = CommandEnvelope::new(
            command_metadata(CommandId::generate(), self.actor, tag, self.now, None),
            CreateConversation {
                conversation_id,
                workspace_id: None,
                kind: ConversationKind::Direct,
                title: Some(DisplayText::new("Input linkage conformance").expect("title")),
                control_home: RuntimeHome::Cloud,
            },
        )
        .expect("create conversation command");
        let ack = self
            .application
            .create_conversation(command)
            .await
            .expect("create conversation");
        assert_eq!(ack.disposition, CommandDisposition::Committed);
        let view = self
            .application
            .get_conversation(GetConversation {
                actor: self.actor,
                conversation_id,
            })
            .await
            .expect("get conversation");
        let user_participant = view
            .participants
            .iter()
            .find(|participant| participant.kind == ParticipantKind::User)
            .expect("user participant")
            .participant_id;
        (conversation_id, user_participant)
    }

    pub async fn current_guard(&self, conversation_id: ConversationId) -> ConversationGuard {
        let view = self
            .application
            .get_conversation(GetConversation {
                actor: self.actor,
                conversation_id,
            })
            .await
            .expect("load conversation guard");
        ConversationGuard {
            conversation_id,
            expected_home_epoch: view.conversation.home_epoch,
            expected_conversation_seq: Some(
                ConversationSeq::new(
                    view.conversation
                        .next_conversation_seq
                        .get()
                        .saturating_sub(1),
                )
                .expect("current conversation seq"),
            ),
        }
    }

    pub async fn submit_user_message(
        &self,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        idempotency: &str,
        text: &str,
    ) -> (MessageId, RunId) {
        let guard = self.current_guard(conversation_id).await;
        let command_id = CommandId::generate();
        let message_id = MessageId::generate();
        let run_id = derive_admission_run_id(command_id).expect("derive admission run id");
        let command = CommandEnvelope::new(
            command_metadata(command_id, self.actor, idempotency, self.now, Some(guard)),
            SubmitMessage {
                project_task_target: None,
                message_id,
                participant_id,
                content: InputContent::InlineUtf8(InlineInput::new(text).expect("inline text")),
                attachment_refs: Vec::new(),
                requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
                requested_features: RequestedFeatures::default(),
                execution_scope: ExecutionScope::Tenant {
                    tenant_id: self.actor.tenant_id,
                },
            },
        )
        .expect("submit command");
        let ack = self
            .application
            .submit_message_authorized(
                command,
                signed_test_grant(self.actor, run_id, self.now),
                AdmissionSelection::web(BTreeSet::new()),
            )
            .await
            .expect("submit message authorized");
        assert_eq!(ack.disposition, CommandDisposition::Committed);
        (message_id, run_id)
    }

    pub async fn enqueue_steering_control(
        &self,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        run_id: RunId,
        idempotency: &str,
        text: &str,
    ) -> MessageId {
        let guard = self.current_guard(conversation_id).await;
        let message_id = MessageId::generate();
        let command = CommandEnvelope::new(
            command_metadata(
                CommandId::generate(),
                self.actor,
                idempotency,
                self.now,
                Some(guard),
            ),
            EnqueueRunControl {
                run_id,
                message_id,
                participant_id,
                kind: RunControlKind::Steering,
                content: InputContent::InlineUtf8(InlineInput::new(text).expect("steering text")),
                attachment_refs: Vec::new(),
            },
        )
        .expect("enqueue steering command");
        let ack = self
            .application
            .enqueue_run_control(command)
            .await
            .expect("enqueue steering");
        assert_eq!(ack.disposition, CommandDisposition::Committed);
        message_id
    }

    pub async fn claim_run(&self, expected_run_id: RunId, worker_tag: &str) -> AttemptContext {
        let scheduler =
            PostgresDispatchStore::new(self.pool.clone(), Arc::new(ProtobufSchedulerEventEncoder));
        let ClaimOutcome::Claimed(claimed) = scheduler
            .claim_one(
                &WorkerId::new(worker_tag).expect("worker id"),
                Duration::from_secs(120),
            )
            .await
            .expect("claim queued run")
        else {
            panic!("expected the queued Run to be claimable");
        };
        assert_eq!(claimed.fence.run_id, expected_run_id);
        AttemptContext {
            attempt_id: claimed.fence.attempt_id,
            run_id: claimed.fence.run_id,
            attempt_number: claimed.fence.attempt_number,
            worker_id: claimed.fence.worker_id,
            lease_id: claimed.fence.lease_id,
            fencing_token: claimed.fence.fencing_token,
            lease_expires_at: claimed.lease_expires_at,
        }
    }

    pub async fn claim_and_fail_run(&self, run_id: RunId, worker_tag: &str) {
        let attempt = self.claim_run(run_id, worker_tag).await;
        self.fail_claimed_attempt(&attempt).await;
    }

    pub async fn fail_claimed_attempt(&self, attempt: &AttemptContext) {
        let run_id = attempt.run_id;
        let audit = Arc::new(PostgresSecurityAuditPipeline::standard(self.pool.clone()));
        let codec = Arc::new(ProtobufLedgerPayloadCodec);
        let journal = musteragent_conformance::fixtures::model_journal::supervise(
            PostgresAgentLoopJournal::with_security_audit(
                self.pool.clone(),
                Arc::clone(&self.content),
                codec.clone(),
                codec,
                Arc::new(ProtobufMeteringPublicationEncoder),
                Arc::clone(&self.keys),
                Arc::clone(&self.recovery_keys),
                audit,
            ),
        );
        let outcome = journal
            .fail(
                attempt,
                ErrorCode::CapabilityUnavailable,
                false,
                None,
                NonZeroU32::new(8).expect("max attempts").into(),
            )
            .await
            .expect("fail claimed attempt");
        assert_eq!(outcome.phase, musteragent_loop::LoopPhase::Failed);
        let view = RunApplication::get_run(
            &self.application,
            GetRun {
                actor: self.actor,
                run_id,
            },
        )
        .await
        .expect("read failed run");
        assert_eq!(view.run.state.status(), RunStatus::Failed);
    }

    pub async fn retry_failed_run(
        &self,
        conversation_id: ConversationId,
        participant_id: ParticipantId,
        source_run_id: RunId,
        source_message_id: MessageId,
        expected_attempt_number: u32,
        idempotency: &str,
    ) -> RunId {
        let guard = self.current_guard(conversation_id).await;
        let command_id = CommandId::generate();
        let retry_run_id = derive_admission_run_id(command_id).expect("derive retry run id");
        let command = CommandEnvelope::new(
            command_metadata(command_id, self.actor, idempotency, self.now, Some(guard)),
            RetryRun {
                source_run_id,
                source_message_id,
                expected_attempt_number: AttemptNumber::new(expected_attempt_number)
                    .expect("attempt number"),
                participant_id,
                requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
                execution_scope: ExecutionScope::Tenant {
                    tenant_id: self.actor.tenant_id,
                },
            },
        )
        .expect("retry command");
        let ack = self
            .application
            .retry_run_authorized(
                command,
                signed_test_grant(self.actor, retry_run_id, self.now),
                AdmissionSelection::web(BTreeSet::new()),
            )
            .await
            .expect("retry failed run");
        assert_eq!(ack.disposition, CommandDisposition::Committed);
        retry_run_id
    }

    pub async fn spawn_delegated_child(
        &self,
        parent_attempt: &AttemptContext,
        task: &str,
    ) -> RunId {
        let parent_view = RunApplication::get_run(
            &self.application,
            GetRun {
                actor: self.actor,
                run_id: parent_attempt.run_id,
            },
        )
        .await
        .expect("read parent run");
        let agent_uuid: uuid::Uuid = sqlx::query_scalar(
            "SELECT agent_id FROM agent.agent_definitions WHERE agent_key = 'assistant'",
        )
        .fetch_one(&self.pool)
        .await
        .expect("read host agent id");
        let agent_id = AgentId::from_uuid(agent_uuid).expect("agent id");
        let request_bytes = serde_json::to_vec(&serde_json::json!({
            "task": task,
            "placement": "cloud",
        }))
        .expect("serialize spawn request");
        let request_ref = self
            .content
            .put(PutContent {
                tenant_id: self.actor.tenant_id,
                media_type: MediaType::new("application/json").expect("json media"),
                bytes: request_bytes,
            })
            .await
            .expect("store spawn request");
        let invocation_id = InvocationId::generate();
        let tool_call_id = ToolCallId::new(format!("call-{invocation_id}")).expect("tool call id");
        let tool_store =
            PostgresToolInvocationStore::new(self.pool.clone(), Arc::clone(&self.keys));
        let tool_invocation = ToolInvocation {
            invocation_id,
            run_id: parent_attempt.run_id,
            requested_by_attempt_id: parent_attempt.attempt_id,
            execution_attempt_id: None,
            tool_call_id: tool_call_id.clone(),
            tool_key: ToolKey::from_versioned_key(
                VersionedKey::new("delegation.spawn@5").expect("delegation tool key"),
            ),
            capability_provider_id: test_runtime_provider_id(),
            status: ToolInvocationStatus::Requested,
            effect: Effect::WriteReversible,
            approval: ApprovalRequirement::Never,
            idempotency_key: IdempotencyKey::new(format!("spawn:{invocation_id}"))
                .expect("idempotency"),
            request_ref: request_ref.clone(),
            result_ref: None,
            interaction_id: None,
            timing: InvocationExecutionTiming::LegacyFixed {
                deadline: self.now.checked_add_millis(60_000).expect("deadline"),
            },
        };
        assert_eq!(
            tool_store
                .begin(
                    self.actor.tenant_id,
                    parent_view.run.context.conversation_id,
                    tool_invocation,
                    ToolRetryContract::IdempotentByKey,
                )
                .await
                .expect("begin delegation tool invocation"),
            ToolPersistenceDisposition::Applied
        );
        tool_store
            .start(
                self.actor.tenant_id,
                invocation_id,
                parent_attempt.attempt_id,
                self.now,
            )
            .await
            .expect("start delegation tool invocation");

        let delegation = PostgresCloudDelegation::new(
            self.pool.clone(),
            Arc::clone(&self.content),
            Arc::new(ProtobufLedgerPayloadCodec),
            Arc::clone(&self.keys),
            Arc::new(SystemLivenessHomeClock::default()),
        );
        let schema_bytes = br#"{"type":"object"}"#;
        let result = delegation
            .invoke(
                parent_attempt,
                DialogueToolInvocation {
                    actor: self.actor,
                    agent_id,
                    run_context: parent_view.run.context,
                    invocation_id,
                    tool_call_id,
                    tool_key: "delegation.spawn@5".to_owned(),
                    request_ref,
                    result_schema: SchemaRef {
                        schema_key: VersionedKey::new("delegation.spawn.result@5")
                            .expect("result schema key"),
                        schema_hash: Sha256::digest(schema_bytes),
                    },
                    requested_at: self.now,
                },
            )
            .await
            .expect("invoke cloud delegation spawn");
        let result_bytes = self
            .content
            .read(self.actor.tenant_id, &result.result_ref)
            .await
            .expect("read delegation spawn result");
        let json: serde_json::Value =
            serde_json::from_slice(&result_bytes).expect("parse spawn result json");
        let child_uuid = uuid::Uuid::parse_str(
            json.get("run_id")
                .and_then(serde_json::Value::as_str)
                .expect("spawned child run_id"),
        )
        .expect("child run uuid");
        RunId::from_uuid(child_uuid).expect("child RunId")
    }

    pub async fn stop_run_graph(&self, root_run_id: RunId) {
        let delegation = PostgresCloudDelegation::new(
            self.pool.clone(),
            Arc::clone(&self.content),
            Arc::new(ProtobufLedgerPayloadCodec),
            Arc::clone(&self.keys),
            Arc::new(SystemLivenessHomeClock::default()),
        );
        delegation
            .stop_run(self.actor, root_run_id, CommandId::generate(), self.now)
            .await
            .expect("stop run graph");
    }

    pub async fn acknowledge_cancelled_attempt(&self, attempt: &AttemptContext) {
        let audit = Arc::new(PostgresSecurityAuditPipeline::standard(self.pool.clone()));
        let codec = Arc::new(ProtobufLedgerPayloadCodec);
        let journal = musteragent_conformance::fixtures::model_journal::supervise(
            PostgresAgentLoopJournal::with_security_audit(
                self.pool.clone(),
                Arc::clone(&self.content),
                codec.clone(),
                codec,
                Arc::new(ProtobufMeteringPublicationEncoder),
                Arc::clone(&self.keys),
                Arc::clone(&self.recovery_keys),
                audit,
            ),
        );
        musteragent_scheduler::AttemptCancellationStore::acknowledge_user_cancellation(
            &journal, attempt,
        )
        .await
        .expect("acknowledge user cancellation");
    }

    pub async fn cancel_queued_run(
        &self,
        conversation_id: ConversationId,
        run_id: RunId,
        tag: &str,
    ) {
        let guard = self.current_guard(conversation_id).await;
        let ack = self
            .application
            .cancel_run(
                CommandEnvelope::new(
                    command_metadata(
                        CommandId::generate(),
                        self.actor,
                        tag,
                        self.now,
                        Some(guard),
                    ),
                    CancelRun {
                        run_id,
                        reason: CancelReason::UserRequested,
                    },
                )
                .expect("cancel command"),
            )
            .await
            .expect("cancel queued run");
        assert_eq!(ack.disposition, CommandDisposition::Committed);
    }

    pub async fn retract_message(
        &self,
        conversation_id: ConversationId,
        message_id: MessageId,
        tag: &str,
    ) {
        let guard = self.current_guard(conversation_id).await;
        let ack = self
            .application
            .retract_message(
                CommandEnvelope::new(
                    command_metadata(
                        CommandId::generate(),
                        self.actor,
                        tag,
                        self.now,
                        Some(guard),
                    ),
                    RetractMessage {
                        message_id,
                        reason: SafeDisplayText::new("user requested rewind").expect("reason"),
                    },
                )
                .expect("retract command"),
            )
            .await
            .expect("retract message");
        assert_eq!(ack.disposition, CommandDisposition::Committed);
    }

    pub async fn admit_coordination_run(
        &self,
        source_conversation: ConversationId,
        target_conversation: ConversationId,
        body: &str,
    ) -> RunId {
        let mailbox = InterSessionMessageId::generate();
        let command_id = CommandId::generate();
        let trace =
            command_metadata(command_id, self.actor, "coord-trace", self.now, None).trace_context;
        let deadline = self
            .now
            .checked_add_millis(30_000)
            .expect("coordination deadline");
        let prepared = self
            .application
            .prepare_coordination_turn(
                self.actor,
                source_conversation,
                target_conversation,
                mailbox,
                command_id,
                deadline,
                trace,
            )
            .await
            .expect("prepare coordination turn");
        let run_id = prepared.target_run_id();
        let mut grant = signed_test_grant(self.actor, run_id, self.now);
        grant.claims.nonce = OpaqueExternalRef::new(coordination_target_grant_nonce(
            source_conversation,
            target_conversation,
            mailbox,
            command_id,
            run_id,
        ))
        .expect("coordination grant nonce");
        let receipt = self
            .application
            .admit_coordination_turn(
                prepared,
                InputContent::InlineUtf8(InlineInput::new(body).expect("coordination body")),
                grant,
            )
            .await
            .expect("admit coordination turn");
        assert_eq!(
            receipt.disposition,
            musteragent_ledger::AppendDisposition::Committed
        );
        run_id
    }

    pub async fn read_projection(
        &self,
        conversation_id: ConversationId,
    ) -> musteragent_projection::ConversationProjectionSnapshot {
        self.projection
            .get_conversation_projection(GetConversationProjection {
                actor: self.actor,
                conversation_id,
                preferred_snapshot_id: None,
            })
            .await
            .expect("read conversation projection")
    }
}

pub fn command_metadata(
    command_id: CommandId,
    actor: Actor,
    idempotency_key: &str,
    now: UtcTimestamp,
    guard: Option<ConversationGuard>,
) -> CommandMetadata {
    CommandMetadata {
        schema_version: DottedKey::new("musteragent.runtime.command.v2").expect("schema"),
        command_id,
        actor,
        idempotency_key: IdempotencyKey::new(idempotency_key).expect("idempotency"),
        issued_at: now,
        deadline: now.checked_add_millis(30_000).expect("deadline"),
        correlation_id: CorrelationId::generate(),
        causation_id: None,
        conversation_guard: guard,
        trace_context: TraceContext {
            traceparent: TraceParent::new(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )
            .expect("traceparent"),
            tracestate: None,
        },
    }
}

pub fn signed_test_grant(actor: Actor, run_id: RunId, now: UtcTimestamp) -> SignedExecutionGrant {
    let tenant_id = actor.tenant_id;
    SignedExecutionGrant {
        context_transport_annex: Vec::new(),
        key_id: DottedKey::new("test.signing-key").expect("key id"),
        claims: ExecutionGrant {
            reviewed_application_source_sha256: Vec::new(),
            product_release: Some(musteragent_tool::compiled_product_release(
                ProductId::KERNEL,
            )),
            schema_version: DottedKey::new(EXECUTION_GRANT_SCHEMA_V10).expect("grant schema"),
            grant_ref: CredentialGrantRef::new(format!("grant_{run_id}")).expect("grant ref"),
            issuer: DottedKey::new("musteragent.backend").expect("issuer"),
            audience: DottedKey::new("musteragent.runtime.cloud").expect("audience"),
            tenant_id,
            model_provider_id: "019fa6de-38fd-7000-8000-000000000421"
                .parse()
                .expect("model provider"),
            model_entry_id: "019fa6de-38fd-7000-8000-000000000422"
                .parse()
                .expect("model entry"),
            run_id,
            issued_at: now,
            expires_at: now.checked_add_millis(60_000).expect("grant expiry"),
            nonce: OpaqueExternalRef::new(format!("nonce_{run_id}")).expect("nonce"),
            requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
            model_route: test_model_route(),
            fallback_model_routes: Vec::new(),
            auxiliary_model_routes: vec![GrantedAuxiliaryModelRoute {
                role: AuxiliaryModelRole::Compression,
                route: test_compression_route(),
                credential_grant_ref: CredentialGrantRef::new(format!(
                    "grant_compression_{run_id}"
                ))
                .expect("compression grant"),
            }],
            agent_contract: Some(
                VersionedKey::new("musteragent.runtime-contract@1").expect("agent contract"),
            ),
            surface: Some(DottedKey::new("web").expect("surface")),
            capability_entitlements: test_capability_entitlements(),
            identity_binding: Some(ExecutionGrantIdentityBinding {
                principal_id: actor.principal_id,
                authority_scope: ExecutionGrantAuthorityScope::Tenant { tenant_id },
                device_id: None,
                device_workspace_id: None,
            }),
            permission_mode: Some(PermissionMode::ServiceManaged),
            device_direct_provider: None,
            vision_direct_provider: None,
            tool_contract_releases: Vec::new(),
        },
        signature: [0; 64],
    }
}

fn test_model_route() -> ModelRoute {
    ModelRoute {
        route_ref: VersionedKey::new("cloud.chat-primary@1").expect("route"),
        resolved_model_key: "gpt-runtime-test".into(),
        model_provider_id: "019fa6de-38fd-7000-8000-000000000421"
            .parse()
            .expect("model provider"),
        model_entry_id: "019fa6de-38fd-7000-8000-000000000422"
            .parse()
            .expect("model entry"),
        api_format: ApiFormat::OpenAiResponses,
        adapter_version: VersionedKey::new("openai.responses-adapter@1").expect("adapter"),
        protocol_version: VersionedKey::new("openai.responses@1").expect("protocol"),
        context_window: NonZeroU64::new(32_768).expect("context"),
        max_output_tokens: NonZeroU64::new(2_048).expect("output"),
        capabilities: BTreeSet::new(),
        parameters: BTreeMap::new(),
        fallback_route_refs: Vec::new(),
        pricing_version_ref: PricingVersionRef::new("catalog:test.price@1").expect("price"),
        provider_state_policy: ProviderStatePolicy::OpaqueContinuation,
    }
}

fn test_compression_route() -> ModelRoute {
    let mut route = test_model_route();
    route.route_ref = VersionedKey::new("cloud.compression@1").expect("compression route");
    route.resolved_model_key = "gpt-runtime-test-compression".into();
    route.model_entry_id = "019fa6de-38fd-7000-8000-000000000423"
        .parse()
        .expect("compression model entry");
    route
}

fn test_runtime_provider_id() -> CapabilityProviderId {
    CapabilityProviderId::from_uuid(
        uuid::Uuid::parse_str("019b2f80-0000-7000-8000-000000000001").expect("provider uuid"),
    )
    .expect("provider id")
}

fn test_backend_provider_id() -> CapabilityProviderId {
    CapabilityProviderId::from_uuid(
        uuid::Uuid::parse_str("019b2f80-0000-7000-8000-000000000003").expect("provider uuid"),
    )
    .expect("provider id")
}

fn test_capability_entitlements() -> Vec<CapabilityEntitlement> {
    const AUTHORIZED_KEYS: [&str; 11] = [
        "core.time.read@1",
        "interaction.request@2",
        "plan.get@2",
        "plan.update@2",
        "skills.list@2",
        "skills.search@2",
        "skills.view@2",
        "tool.catalog.activate@2",
        "tool.catalog.search@2",
        "vision.analyze@4",
        "web.fetch@2",
    ];
    let catalog = BuiltinToolCatalog::load_cloud().expect("cloud tool catalog");
    let mut entitlements = catalog
        .definitions()
        .iter()
        .filter(|definition| AUTHORIZED_KEYS.contains(&definition.capability_key.as_str()))
        .map(|definition| {
            let (provider_id, location) = match definition.target {
                CapabilityExecutionTarget::RustRuntime => {
                    (test_runtime_provider_id(), CapabilityLocation::Cloud)
                }
                CapabilityExecutionTarget::BackendNamed(_) => {
                    (test_backend_provider_id(), CapabilityLocation::Backend)
                }
            };
            CapabilityEntitlement {
                key: CapabilityKey::from_versioned_key(definition.capability_key.clone()),
                provider_id,
                location,
                effect: definition.effect,
                allowed_scopes: definition.required_scopes.clone(),
                policy_decision: Decision::Allow,
            }
        })
        .collect::<Vec<_>>();
    entitlements.sort_by(|left, right| left.key.cmp(&right.key));
    entitlements
}

async fn publish_host_agent(pool: &PgPool, actor: Actor, content: &dyn ContentStore) {
    let mut definition = musteragent_conformance::fixtures::agent::system_definition();
    definition.kind = musteragent_definition::AgentKind::Custom;
    definition.built_in = false;
    definition.system_slot = None;
    definition.default_for_product = false;
    definition.deletable = true;
    definition.replaceable = true;
    let version = musteragent_conformance::fixtures::agent::system_version(1, "assistant-base@1");
    let material = version.material.as_ref().expect("host material");
    for (reference, bytes) in [
        (
            &version.persona_content_ref,
            b"synthetic persona".as_slice(),
        ),
        (
            &material.policy_content_ref,
            b"synthetic runtime policy".as_slice(),
        ),
        (
            &material.memory_policy_content_ref,
            b"synthetic memory policy".as_slice(),
        ),
        (
            &material.manifest_ref,
            b"synthetic Agent manifest v1".as_slice(),
        ),
    ] {
        let stored: ContentRef = content
            .put_at(
                reference.content_id,
                PutContent {
                    tenant_id: actor.tenant_id,
                    media_type: reference.media_type.clone(),
                    bytes: bytes.to_vec(),
                },
            )
            .await
            .expect("publish fixture content");
        assert_eq!(&stored, reference);
    }
    let registry = PostgresAgentDefinitionStore::new(pool.clone());
    registry
        .insert_definition(definition.clone())
        .await
        .expect("register host Agent");
    registry
        .insert_version(version.clone())
        .await
        .expect("publish host version");
    registry
        .compare_and_swap_published(
            definition.agent_id,
            None,
            PublishedAgentVersionPointer {
                agent_id: definition.agent_id,
                agent_version_id: version.agent_version_id,
                version: 1,
                published_at: version.published_at,
                published_by: actor.principal_id,
            },
        )
        .await
        .expect("publish host pointer");
}
