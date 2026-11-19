use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::Arc;

use musteragent_capability::{CapabilityEntitlement, CapabilityKey, CapabilityLocation};
use musteragent_content_ref::MediaType;
use musteragent_context::RuntimeSnapshotDecoder as _;
use musteragent_conversation::{
    ActivateConversationContextBranch, ArchiveConversation, ContextBranchOrigin,
    ConversationApplication, ConversationKind, ConversationLifecycle, CreateConversation,
    EditAndResubmitMessage, ForkConversationContext, GetConversation, ParticipantKind,
    RetractMessage, SubmitMessage, UnarchiveConversation,
};
use musteragent_event_envelope::{ExecutionScope, LedgerCursor, RuntimeHome};
use musteragent_execution_grant::{
    EXECUTION_GRANT_SCHEMA_V10, ExecutionGrant, ExecutionGrantAuthorityScope,
    ExecutionGrantIdentityBinding, GrantedAuxiliaryModelRoute, SignedExecutionGrant,
};
use musteragent_identity::{
    Actor, AttemptId, CapabilityProviderId, CommandId, ContextBranchId, ConversationId,
    ConversationSeq, CorrelationId, DottedKey, HomeEpoch, IdempotencyKey, InvocationId,
    LedgerRecordId, MessageId, PrincipalId, PrincipalKind, RunId, TenantId, UtcTimestamp,
    VersionedKey,
};
use musteragent_model::{
    ApiFormat, AuxiliaryModelRole, CredentialGrantRef, ModelEntryId, ModelRoute, PricingVersionRef,
    ProviderStatePolicy,
};
use musteragent_object_store::ObjectStoreBlobStore;
use musteragent_policy::Decision;
use musteragent_postgres::{
    PostgresContentStore, PostgresConversationProjection, PostgresInteractionApplicationStore,
    PostgresLedgerStore, PostgresRuntimeApplicationStore, PostgresSecurityAuditPipeline,
};
use musteragent_projection::{
    ConversationProjectionApplication, GetConversationProjection, GetConversationProjectionWindow,
    LedgerPayloadDecoder, PROJECTION_SNAPSHOT_ROOT_KIND, SubscriptionError,
    SubscriptionErrorMapper,
};
use musteragent_run::{CancelRun, EnqueueRunControl, RunApplication, RunControlKind, RunStatus};
use musteragent_runtime_application::{
    ApplicationClock, KernelSnapshotBuilder, RuntimeApplication, RuntimeSnapshotResolution,
    RuntimeSnapshotResolver, SnapshotResolutionRequest, derive_admission_run_id, runtime_error,
    runtime_error_with_diagnostic,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{
    ActiveRunArchivePolicy, ApplicationFuture, CancelReason, CommandDisposition, CommandEnvelope,
    CommandMetadata, ConversationGuard, DisplayText, ErrorCode, ErrorDetails, InlineInput,
    InputContent, ModelClass, OpaqueExternalRef, RequestedFeatures, RuntimeError, SafeDisplayText,
    TraceContext, TraceParent,
};
use musteragent_runtime_snapshot::{RunExecutionPolicySnapshot, RuntimeSnapshot};
use musteragent_scheduler::{
    AdmissionApplication, AdmissionSelection, ExecutionGrantAuthenticator,
};
use musteragent_skill::{
    SkillAuthorizationRequest, SkillCatalogEntry, Surface, builtin_skill_catalog_entries,
};
use musteragent_storage_protection::OpaqueRefKeyring;
use musteragent_testkit::IsolatedPostgres;
use musteragent_tool::{BuiltinToolCatalog, CapabilityExecutionTarget};
use musteragent_wire::ProtobufLedgerPayloadCodec;
use musteragent_wire::{ProtobufConversationProjectionCodec, ProtobufSnapshotCodec};

use musteragent_postgres::migrate;
use musteragent_runtime_skill::authorized_skill_catalog;
include!("support/postgres_runtime_application_support.rs");

include!("support/postgres_runtime_application_resolver.rs");

#[tokio::test]
async fn application_create_and_submit_are_atomic_authorized_and_idempotent() {
    let postgres = IsolatedPostgres::start_with_pgvector()
        .await
        .expect("start PostgreSQL");
    let pool = postgres.pool().await.expect("connect PostgreSQL");
    migrate(&pool).await.expect("migrate");
    let key_id = DottedKey::new("runtime.application.test").expect("key");
    let keys =
        Arc::new(OpaqueRefKeyring::new(key_id.clone(), [(key_id, [41_u8; 32])]).expect("keyring"));
    let content_store = PostgresContentStore::new(
        pool.clone(),
        Arc::new(ObjectStoreBlobStore::in_memory()),
        Arc::clone(&keys),
    );
    let content: Arc<dyn ContentStore> = Arc::new(content_store.clone());
    let tenant_id = TenantId::from_uuid(
        uuid::Uuid::parse_str("019b2f80-0000-7000-8000-000000000002").expect("tenant UUID"),
    )
    .expect("tenant ID");
    let principal_id = PrincipalId::generate();
    let actor = Actor {
        tenant_id,
        principal_id,
        principal_kind: PrincipalKind::User,
    };
    let now = UtcTimestamp::from_unix_millis(1_780_000_000_000);
    publish_host_agent(&pool, actor, content.as_ref()).await;
    let (application_store, interaction_store) =
        production_audit_stores(&pool, keys, content_store);
    let application = RuntimeApplication::new(
        musteragent_identity::AgentKey::new("assistant").expect("Agent key"),
        musteragent_runtime_application::RuntimeApplicationPorts {
            store: application_store.clone(),
            snapshots: Arc::new(TestSnapshotResolver::production(
                &pool,
                Arc::clone(&content),
            )),
            content: Arc::clone(&content),
            encoder: Arc::new(ProtobufLedgerPayloadCodec),
            clock: Arc::new(MovingClock::new(now)),
            grant_authenticator: Arc::new(TestGrantAuthenticator),
            interactions: interaction_store.clone(),
        },
    );
    let conversation_id = ConversationId::generate();
    let create_id = CommandId::generate();
    let create = CommandEnvelope::new(
        metadata(create_id, actor, "create-runtime-application", now, None),
        CreateConversation {
            conversation_id,
            workspace_id: None,
            kind: ConversationKind::Direct,
            title: Some(DisplayText::new("Runtime facade").expect("title")),
            control_home: RuntimeHome::Cloud,
        },
    )
    .expect("create command");
    let first = application
        .create_conversation(create.clone())
        .await
        .expect("create Conversation");
    assert_eq!(first.disposition, CommandDisposition::Committed);
    assert_eq!(
        application
            .create_conversation(create.clone())
            .await
            .expect("duplicate create")
            .disposition,
        CommandDisposition::Duplicate
    );
    let mut conflicting_create = create;
    conflicting_create.payload.title =
        Some(DisplayText::new("Different title").expect("conflicting title"));
    assert_eq!(
        application
            .create_conversation(conflicting_create)
            .await
            .expect_err("changed create payload must conflict")
            .code,
        ErrorCode::IdempotencyConflict
    );
    let view = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation view");
    assert_eq!(view.participants.len(), 2);
    let participant_id = view
        .participants
        .iter()
        .find(|participant| participant.kind == ParticipantKind::User)
        .expect("user participant")
        .participant_id;
    let current_sequence = ConversationSeq::new(
        view.conversation
            .next_conversation_seq
            .get()
            .saturating_sub(1),
    )
    .expect("current sequence");
    assert_denied_admission_audit(
        &application,
        &pool,
        actor,
        conversation_id,
        view.conversation.home_epoch,
        current_sequence,
        now,
    )
    .await;
    assert_stage_denial_audits(
        &pool,
        application_store.clone(),
        interaction_store.clone(),
        Arc::clone(&content),
        actor,
        conversation_id,
        view.conversation.home_epoch,
        current_sequence,
        participant_id,
        now,
    )
    .await;
    let submit_id = CommandId::generate();
    let submitted_message_id = MessageId::generate();
    let submit = CommandEnvelope::new(
        metadata(
            submit_id,
            actor,
            "submit-runtime-application",
            now,
            Some(ConversationGuard {
                conversation_id,
                expected_home_epoch: view.conversation.home_epoch,
                expected_conversation_seq: Some(
                    ConversationSeq::new(
                        view.conversation
                            .next_conversation_seq
                            .get()
                            .saturating_sub(1),
                    )
                    .expect("current sequence"),
                ),
            }),
        ),
        SubmitMessage {
            project_task_target: None,
            message_id: submitted_message_id,
            participant_id,
            content: InputContent::InlineUtf8(
                InlineInput::new("hello from Rust admission").expect("message"),
            ),
            attachment_refs: Vec::new(),
            requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
            requested_features: RequestedFeatures {
                reasoning_summary: true,
                ..RequestedFeatures::default()
            },
            execution_scope: ExecutionScope::Tenant { tenant_id },
        },
    )
    .expect("submit command");
    let run_id = derive_admission_run_id(submit_id).expect("derived Run ID");
    let signed_grant = signed_test_grant(actor, run_id, now);
    let admitted = application
        .submit_message_authorized(
            submit.clone(),
            signed_grant.clone(),
            AdmissionSelection::web(BTreeSet::new()),
        )
        .await
        .expect("admit Run");
    assert_eq!(admitted.disposition, CommandDisposition::Committed);
    let encoded_snapshot: Vec<u8> = sqlx::query_scalar(
        "SELECT encoded_snapshot FROM capability.runtime_snapshots WHERE run_id = $1",
    )
    .bind(run_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read persisted Product Snapshot");
    let snapshot = ProtobufSnapshotCodec
        .decode_runtime_snapshot(&encoded_snapshot)
        .expect("decode persisted Product Snapshot");
    let expected_skills = authorized_test_skills(&test_capability_entitlements());
    assert_eq!(snapshot.skills, expected_skills);
    assert_eq!(snapshot.tools.len(), 11);
    assert_eq!(snapshot.capabilities.len(), 11);
    assert_eq!(
        snapshot
            .discovery
            .as_ref()
            .expect("Product discovery Snapshot")
            .authorized_skill_catalog_hash,
        authorized_test_skill_catalog_hash(&test_capability_entitlements(), &expected_skills),
        "the persisted authorized Skill catalog hash must cover the exact sealed Skill set"
    );
    assert_eq!(
        application
            .submit_message_authorized(
                submit.clone(),
                signed_grant.clone(),
                AdmissionSelection::web(BTreeSet::new()),
            )
            .await
            .expect("duplicate submit")
            .disposition,
        CommandDisposition::Duplicate
    );
    let mut conflicting_submit = submit;
    conflicting_submit.payload.content = InputContent::InlineUtf8(
        InlineInput::new("changed content with the same idempotency key")
            .expect("conflicting content"),
    );
    assert_eq!(
        application
            .submit_message_authorized(
                conflicting_submit,
                signed_grant,
                AdmissionSelection::web(BTreeSet::new()),
            )
            .await
            .expect_err("changed submit payload must conflict")
            .code,
        ErrorCode::IdempotencyConflict
    );
    let admitted_run_id = admitted
        .resource_refs
        .iter()
        .find_map(|resource| match resource {
            musteragent_runtime_contract::ResourceRef::Run(value) => Some(*value),
            _ => None,
        })
        .expect("Run resource");
    assert_eq!(admitted_run_id, run_id);
    assert_admitted_audit(&pool, tenant_id, run_id).await;
    let ledger_records_before_projection: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM event_store.ledger_records WHERE conversation_id = $1",
    )
    .bind(conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("ledger count before projection");
    let projection_ledger_codec = Arc::new(ProtobufLedgerPayloadCodec);
    let product_projection = PostgresConversationProjection::new(
        pool.clone(),
        projection_ledger_codec.clone(),
        Arc::new(ProtobufConversationProjectionCodec),
        Arc::clone(&content),
        Arc::new(TestSubscriptionErrorMapper),
    );
    let projection = product_projection
        .get_conversation_projection(GetConversationProjection {
            actor,
            conversation_id,
            preferred_snapshot_id: None,
        })
        .await
        .expect("Conversation Projection Snapshot");
    let window = product_projection
        .get_product_read_projection_window(GetConversationProjectionWindow {
            actor,
            conversation_id,
            limit: 1,
            before: None,
        })
        .await
        .expect("bounded PostgreSQL Conversation Projection Window");
    assert_eq!(window.head_cursor, projection.cursor());
    assert_eq!(window.projection.messages.len(), 1);
    assert_eq!(
        window.projection.messages[0].message_id,
        projection
            .messages
            .last()
            .expect("projected message")
            .message_id,
    );
    assert!(
        projection
            .messages
            .iter()
            .any(|message| message.message_id == submitted_message_id)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_store.ledger_records WHERE conversation_id = $1",
        )
        .bind(conversation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("ledger count after projection"),
        ledger_records_before_projection,
        "building a product Snapshot must never truncate or rewrite Ledger"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_store.snapshots
             WHERE conversation_id = $1
               AND snapshot_kind = $2",
        )
        .bind(conversation_id.into_uuid())
        .bind(PROJECTION_SNAPSHOT_ROOT_KIND)
        .fetch_one(&pool)
        .await
        .expect("projection snapshot count"),
        1
    );
    let run = musteragent_run::RunApplication::get_run(
        &application,
        musteragent_run::GetRun { actor, run_id },
    )
    .await
    .expect("Run view");
    assert_eq!(run.run.context.run_id, run_id);
    assert_eq!(run.run.state.status(), musteragent_run::RunStatus::Queued);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM runtime.execution_grant_consumptions WHERE run_id = $1",
        )
        .bind(run_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("grant count"),
        1
    );
    let after_admission = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation after admission");
    let retract_while_active = CommandEnvelope::new(
        metadata(
            CommandId::generate(),
            actor,
            "retract-active-runtime-application",
            now,
            Some(ConversationGuard {
                conversation_id,
                expected_home_epoch: after_admission.conversation.home_epoch,
                expected_conversation_seq: Some(
                    ConversationSeq::new(
                        after_admission
                            .conversation
                            .next_conversation_seq
                            .get()
                            .saturating_sub(1),
                    )
                    .expect("current sequence"),
                ),
            }),
        ),
        RetractMessage {
            message_id: submitted_message_id,
            reason: SafeDisplayText::new("must cancel first").expect("reason"),
        },
    )
    .expect("active retraction command");
    assert_eq!(
        application
            .retract_message(retract_while_active)
            .await
            .expect_err("active Run must block retraction")
            .code,
        ErrorCode::RunTransitionInvalid
    );
    let cancel = CommandEnvelope::new(
        metadata(
            CommandId::generate(),
            actor,
            "cancel-runtime-application",
            now,
            Some(ConversationGuard {
                conversation_id,
                expected_home_epoch: after_admission.conversation.home_epoch,
                expected_conversation_seq: Some(
                    ConversationSeq::new(
                        after_admission
                            .conversation
                            .next_conversation_seq
                            .get()
                            .saturating_sub(2),
                    )
                    .expect("stale sequence while the Run advances"),
                ),
            }),
        ),
        CancelRun {
            run_id,
            reason: CancelReason::UserRequested,
        },
    )
    .expect("cancel command");
    assert_eq!(
        application
            .cancel_run(cancel.clone())
            .await
            .expect("cancel queued Run")
            .disposition,
        CommandDisposition::Committed
    );
    assert_eq!(
        application
            .cancel_run(cancel)
            .await
            .expect("duplicate cancel")
            .disposition,
        CommandDisposition::Duplicate
    );
    assert_eq!(
        application
            .get_run(musteragent_run::GetRun { actor, run_id })
            .await
            .expect("cancelled Run")
            .run
            .state
            .status(),
        RunStatus::Cancelled
    );

    let after_cancel = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation after cancellation");
    let projected_assistant_message_id = MessageId::generate();
    let request_ref = content
        .put(PutContent {
            tenant_id,
            media_type: MediaType::new("application/json").expect("request media"),
            bytes: br#"{"input":"failed tool continuation"}"#.to_vec(),
        })
        .await
        .expect("store failed invocation request");
    // Multi-step Agent loops reuse their logical Assistant message identity
    // across model invocations. Retraction must emit that message exactly once.
    for invocation_index in 0..3 {
        let invocation_id = InvocationId::generate();
        sqlx::query(
            "INSERT INTO runtime.model_invocations (
                 invocation_id, tenant_id, conversation_id, run_id, attempt_id,
                 purpose, route_role, status, model_route_ref, credential_grant_ref,
                 requested_model_key,
                 pricing_version_ref, idempotency_key, request_content_id,
                 request_hash, assistant_message_id, started_conversation_seq,
                 started_at_ms, finished_at_ms
             ) VALUES (
                 $1, $2, $3, $4, $5, 1, 0, 3, 'cloud.chat-primary@1',
                 'grant_runtime_application_fixture', 'gpt-runtime-test',
                 'catalog:test.price@1', $6, $7, $8, $9,
                 $10::numeric, $11, $11
             )",
        )
        .bind(invocation_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(run_id.into_uuid())
        .bind(AttemptId::generate().into_uuid())
        .bind(format!(
            "failed-projected-output:{projected_assistant_message_id}:{invocation_index}"
        ))
        .bind(request_ref.content_id.into_uuid())
        .bind(request_ref.sha256.into_bytes().to_vec())
        .bind(projected_assistant_message_id.into_uuid())
        .bind(
            after_cancel
                .conversation
                .next_conversation_seq
                .get()
                .saturating_sub(1)
                .to_string(),
        )
        .bind(now.unix_millis())
        .execute(&pool)
        .await
        .expect("seed repeated invocation-only Assistant output");
    }
    let retract = CommandEnvelope::new(
        metadata(
            CommandId::generate(),
            actor,
            "retract-runtime-application",
            now,
            Some(ConversationGuard {
                conversation_id,
                expected_home_epoch: after_cancel.conversation.home_epoch,
                expected_conversation_seq: Some(
                    ConversationSeq::new(
                        after_cancel
                            .conversation
                            .next_conversation_seq
                            .get()
                            .saturating_sub(1),
                    )
                    .expect("current sequence"),
                ),
            }),
        ),
        RetractMessage {
            message_id: submitted_message_id,
            reason: SafeDisplayText::new("user requested rewind").expect("reason"),
        },
    )
    .expect("retract command");
    let retracted = application
        .retract_message(retract.clone())
        .await
        .expect("retract message suffix");
    assert_eq!(retracted.disposition, CommandDisposition::Committed);
    assert_eq!(
        retracted.resource_refs,
        [
            musteragent_runtime_contract::ResourceRef::Message(submitted_message_id),
            musteragent_runtime_contract::ResourceRef::Message(projected_assistant_message_id),
        ]
    );
    assert_eq!(
        application
            .retract_message(retract)
            .await
            .expect("duplicate retraction")
            .disposition,
        CommandDisposition::Duplicate
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM conversation.messages
             WHERE conversation_id = $1 AND lifecycle = 3",
        )
        .bind(conversation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("retracted message count"),
        1
    );
    let after_retract = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation after retraction");
    let preserved_branch_id = ContextBranchId::generate();
    let replacement_branch_id = ContextBranchId::generate();
    let current_offset = after_retract
        .conversation
        .next_conversation_seq
        .get()
        .saturating_sub(1);
    let preserved_fork_record_id = LedgerRecordId::from_uuid(
        sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT record_id FROM event_store.ledger_records
             WHERE tenant_id = $1 AND conversation_id = $2
               AND conversation_seq = $3::numeric",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(current_offset.to_string())
        .fetch_one(&pool)
        .await
        .expect("current branch fork record"),
    )
    .expect("current branch fork record ID");
    let replacement_fork_record_id = LedgerRecordId::from_uuid(
        sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT record_id FROM event_store.ledger_records
             WHERE tenant_id = $1 AND conversation_id = $2
               AND conversation_seq = 3",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("replacement fork record"),
    )
    .expect("replacement fork record ID");
    let preserved_fork = CommandEnvelope::new(
        metadata(
            CommandId::generate(),
            actor,
            "fork-preserved-context",
            now,
            Some(ConversationGuard {
                conversation_id,
                expected_home_epoch: after_retract.conversation.home_epoch,
                expected_conversation_seq: Some(
                    ConversationSeq::new(current_offset).expect("current offset"),
                ),
            }),
        ),
        ForkConversationContext {
            branch_id: preserved_branch_id,
            parent_branch_id: None,
            fork_from_record_id: preserved_fork_record_id,
            branch_summary_ref: None,
        },
    )
    .expect("preserved branch command");
    assert_eq!(
        application
            .fork_context(preserved_fork.clone())
            .await
            .expect("fork preserved path")
            .disposition,
        CommandDisposition::Committed
    );
    assert_eq!(
        application
            .fork_context(preserved_fork)
            .await
            .expect("duplicate preserved fork")
            .disposition,
        CommandDisposition::Duplicate
    );
    let after_preserved_fork = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation after preserved fork");
    application
        .fork_context(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    actor,
                    "fork-replacement-context",
                    now,
                    Some(ConversationGuard {
                        conversation_id,
                        expected_home_epoch: after_preserved_fork.conversation.home_epoch,
                        expected_conversation_seq: Some(
                            ConversationSeq::new(
                                after_preserved_fork
                                    .conversation
                                    .next_conversation_seq
                                    .get()
                                    .saturating_sub(1),
                            )
                            .expect("current sequence"),
                        ),
                    }),
                ),
                ForkConversationContext {
                    branch_id: replacement_branch_id,
                    parent_branch_id: None,
                    fork_from_record_id: replacement_fork_record_id,
                    branch_summary_ref: None,
                },
            )
            .expect("replacement branch command"),
        )
        .await
        .expect("fork replacement path");
    for (index, branch_id) in [
        replacement_branch_id,
        preserved_branch_id,
        replacement_branch_id,
    ]
    .into_iter()
    .enumerate()
    {
        let before_activation = application
            .get_conversation(GetConversation {
                actor,
                conversation_id,
            })
            .await
            .expect("Conversation before branch activation");
        let activation = CommandEnvelope::new(
            metadata(
                CommandId::generate(),
                actor,
                &format!("activate-context-branch-{index}"),
                now,
                Some(ConversationGuard {
                    conversation_id,
                    expected_home_epoch: before_activation.conversation.home_epoch,
                    expected_conversation_seq: Some(
                        ConversationSeq::new(
                            before_activation
                                .conversation
                                .next_conversation_seq
                                .get()
                                .saturating_sub(1),
                        )
                        .expect("current sequence"),
                    ),
                }),
            ),
            ActivateConversationContextBranch { branch_id },
        )
        .expect("activate branch command");
        application
            .activate_context_branch(activation.clone())
            .await
            .expect("activate Context branch");
        if index == 0 {
            assert_eq!(
                application
                    .activate_context_branch(activation)
                    .await
                    .expect("duplicate Context branch activation")
                    .disposition,
                CommandDisposition::Duplicate
            );
        }
    }
    assert_eq!(
        sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT active_branch_id FROM conversation.context_branch_state
             WHERE tenant_id = $1 AND conversation_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("active branch state"),
        replacement_branch_id.into_uuid()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM conversation.context_branches
             WHERE tenant_id = $1 AND conversation_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("Context branch count"),
        2
    );
    assert!(
        product_projection
            .get_conversation_projection(GetConversationProjection {
                actor,
                conversation_id,
                preferred_snapshot_id: Some(projection.snapshot_id),
            })
            .await
            .is_err(),
        "an exact root Snapshot ID must not fall back after branch activation"
    );
    let branched_projection = product_projection
        .get_conversation_projection(GetConversationProjection {
            actor,
            conversation_id,
            preferred_snapshot_id: None,
        })
        .await
        .expect("active-branch product Snapshot");
    let branched_head = sqlx::query_scalar::<_, String>(
        "SELECT last_sequence::text FROM event_store.ledger_streams
         WHERE tenant_id = $1 AND conversation_id = $2",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("branched projection stream head")
    .parse::<u64>()
    .expect("branched projection stream head number");
    assert_ne!(branched_projection.snapshot_id, projection.snapshot_id);
    assert_eq!(
        branched_projection.through_conversation_seq.get(),
        branched_head,
        "a branched product projection must be reduced to the stream head instead of exposing a mixed raw tail"
    );
    let tail = PostgresLedgerStore::new(pool.clone())
        .load(
            LedgerCursor {
                conversation_id,
                home_epoch: projection.home_epoch,
                after_conversation_seq: projection.through_conversation_seq,
            },
            1_000,
        )
        .await
        .expect("load retraction tail");
    let retracted_ids = tail
        .records
        .iter()
        .filter_map(|record| {
            let payload = projection_ledger_codec
                .decode(record.metadata.record_kind, &record.payload)
                .expect("decode retraction tail");
            match payload {
                musteragent_ledger::LedgerPayload::DomainEvent(
                    musteragent_ledger::DomainEvent::Conversation(
                        musteragent_conversation::ConversationEvent::MessageRetracted {
                            message_id,
                            ..
                        },
                    ),
                ) => Some(message_id),
                _ => None,
            }
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        retracted_ids,
        BTreeSet::from([submitted_message_id, projected_assistant_message_id]),
        "the durable Tail after a cached Snapshot must retract both User and derived Assistant output"
    );
    let after_branch_navigation = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation after branch navigation");
    let second_submit_id = CommandId::generate();
    let second_run_id = derive_admission_run_id(second_submit_id).expect("second Run ID");
    let second_message_id = MessageId::generate();
    let second_submit = CommandEnvelope::new(
        metadata(
            second_submit_id,
            actor,
            "submit-before-archive",
            now,
            Some(ConversationGuard {
                conversation_id,
                expected_home_epoch: after_branch_navigation.conversation.home_epoch,
                expected_conversation_seq: Some(
                    ConversationSeq::new(
                        after_branch_navigation
                            .conversation
                            .next_conversation_seq
                            .get()
                            .saturating_sub(1),
                    )
                    .expect("current sequence"),
                ),
            }),
        ),
        SubmitMessage {
            project_task_target: None,
            message_id: second_message_id,
            participant_id,
            content: InputContent::InlineUtf8(
                InlineInput::new("archive this active Run").expect("message"),
            ),
            attachment_refs: Vec::new(),
            requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
            requested_features: RequestedFeatures::default(),
            execution_scope: ExecutionScope::Tenant { tenant_id },
        },
    )
    .expect("second submit");
    let second_admission = application
        .submit_message_authorized(
            second_submit,
            signed_test_grant(actor, second_run_id, now),
            AdmissionSelection::web(BTreeSet::new()),
        )
        .await
        .expect("admit second Run");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_store.ledger_records
             WHERE tenant_id = $1 AND conversation_id = $2
               AND conversation_seq BETWEEN $3::numeric AND $4::numeric
               AND context_branch_id = $5",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(
            second_admission
                .first_conversation_seq
                .expect("second admission first sequence")
                .get()
                .to_string(),
        )
        .bind(
            second_admission
                .last_conversation_seq
                .expect("second admission last sequence")
                .get()
                .to_string(),
        )
        .bind(replacement_branch_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("second admission branch tags"),
        3,
        "every fact appended after activation must be attached to the active branch"
    );
    let after_second_admission = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation after second admission");
    application
        .cancel_run(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    actor,
                    "cancel-before-edit-and-resubmit",
                    now,
                    Some(ConversationGuard {
                        conversation_id,
                        expected_home_epoch: after_second_admission.conversation.home_epoch,
                        expected_conversation_seq: Some(
                            ConversationSeq::new(
                                after_second_admission
                                    .conversation
                                    .next_conversation_seq
                                    .get()
                                    .saturating_sub(1),
                            )
                            .expect("current sequence"),
                        ),
                    }),
                ),
                CancelRun {
                    run_id: second_run_id,
                    reason: CancelReason::UserRequested,
                },
            )
            .expect("cancel before edit command"),
        )
        .await
        .expect("cancel before atomic edit");

    // A retraction is an active-branch fact, not a global mutation. Fork two
    // siblings from the same head, retract on one, then prove the other still
    // exposes and can edit the shared User Message.
    let shared_fork_record_id = LedgerRecordId::from_uuid(
        sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT record_id FROM event_store.ledger_records
             WHERE tenant_id = $1 AND conversation_id = $2
             ORDER BY conversation_seq DESC LIMIT 1",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("shared sibling fork record"),
    )
    .expect("shared sibling fork record ID");
    let retracted_sibling_branch_id = ContextBranchId::generate();
    let retained_sibling_branch_id = ContextBranchId::generate();
    for (index, branch_id) in [retracted_sibling_branch_id, retained_sibling_branch_id]
        .into_iter()
        .enumerate()
    {
        let before_fork = application
            .get_conversation(GetConversation {
                actor,
                conversation_id,
            })
            .await
            .expect("Conversation before sibling fork");
        application
            .fork_context(
                CommandEnvelope::new(
                    metadata(
                        CommandId::generate(),
                        actor,
                        &format!("fork-retraction-sibling-{index}"),
                        now,
                        Some(ConversationGuard {
                            conversation_id,
                            expected_home_epoch: before_fork.conversation.home_epoch,
                            expected_conversation_seq: Some(
                                ConversationSeq::new(
                                    before_fork
                                        .conversation
                                        .next_conversation_seq
                                        .get()
                                        .saturating_sub(1),
                                )
                                .expect("current sequence"),
                            ),
                        }),
                    ),
                    ForkConversationContext {
                        branch_id,
                        parent_branch_id: Some(replacement_branch_id),
                        fork_from_record_id: shared_fork_record_id,
                        branch_summary_ref: None,
                    },
                )
                .expect("sibling fork command"),
            )
            .await
            .expect("fork retraction sibling");
    }
    let before_retracted_activation = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation before retracted sibling activation");
    application
        .activate_context_branch(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    actor,
                    "activate-retracted-sibling",
                    now,
                    Some(ConversationGuard {
                        conversation_id,
                        expected_home_epoch: before_retracted_activation.conversation.home_epoch,
                        expected_conversation_seq: Some(
                            ConversationSeq::new(
                                before_retracted_activation
                                    .conversation
                                    .next_conversation_seq
                                    .get()
                                    .saturating_sub(1),
                            )
                            .expect("current sequence"),
                        ),
                    }),
                ),
                ActivateConversationContextBranch {
                    branch_id: retracted_sibling_branch_id,
                },
            )
            .expect("activate retracted sibling command"),
        )
        .await
        .expect("activate retracted sibling");
    let before_branch_retract = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation before branch-local retraction");
    application
        .retract_message(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    actor,
                    "retract-on-one-sibling",
                    now,
                    Some(ConversationGuard {
                        conversation_id,
                        expected_home_epoch: before_branch_retract.conversation.home_epoch,
                        expected_conversation_seq: Some(
                            ConversationSeq::new(
                                before_branch_retract
                                    .conversation
                                    .next_conversation_seq
                                    .get()
                                    .saturating_sub(1),
                            )
                            .expect("current sequence"),
                        ),
                    }),
                ),
                RetractMessage {
                    message_id: second_message_id,
                    reason: SafeDisplayText::new("branch-local rewind").expect("reason"),
                },
            )
            .expect("branch-local retract command"),
        )
        .await
        .expect("retract on one sibling");
    assert_eq!(
        sqlx::query_scalar::<_, Option<uuid::Uuid>>(
            "SELECT context_branch_id FROM conversation.message_retractions
             WHERE tenant_id = $1 AND conversation_id = $2 AND message_id = $3",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(second_message_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("branch-local retraction projection"),
        Some(retracted_sibling_branch_id.into_uuid())
    );
    let retracted_sibling_projection = product_projection
        .get_conversation_projection(GetConversationProjection {
            actor,
            conversation_id,
            preferred_snapshot_id: None,
        })
        .await
        .expect("retracted sibling projection");
    assert!(
        retracted_sibling_projection
            .messages
            .iter()
            .all(|message| message.message_id != second_message_id)
    );
    let before_retained_activation = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation before retained sibling activation");
    application
        .activate_context_branch(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    actor,
                    "activate-retained-sibling",
                    now,
                    Some(ConversationGuard {
                        conversation_id,
                        expected_home_epoch: before_retained_activation.conversation.home_epoch,
                        expected_conversation_seq: Some(
                            ConversationSeq::new(
                                before_retained_activation
                                    .conversation
                                    .next_conversation_seq
                                    .get()
                                    .saturating_sub(1),
                            )
                            .expect("current sequence"),
                        ),
                    }),
                ),
                ActivateConversationContextBranch {
                    branch_id: retained_sibling_branch_id,
                },
            )
            .expect("activate retained sibling command"),
        )
        .await
        .expect("activate retained sibling");
    let retained_sibling_projection = product_projection
        .get_conversation_projection(GetConversationProjection {
            actor,
            conversation_id,
            preferred_snapshot_id: None,
        })
        .await
        .expect("retained sibling projection");
    assert!(
        retained_sibling_projection
            .messages
            .iter()
            .any(|message| message.message_id == second_message_id),
        "a sibling branch must not inherit another branch's retraction"
    );
    let before_edit = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation before atomic edit");
    let edit_command_id = CommandId::generate();
    let edited_run_id = derive_admission_run_id(edit_command_id).expect("edited Run ID");
    let edited_message_id = MessageId::generate();
    let edit_preserved_branch_id = ContextBranchId::generate();
    let edit_replacement_branch_id = ContextBranchId::generate();
    let edit_command = CommandEnvelope::new(
        metadata(
            edit_command_id,
            actor,
            "atomic-edit-and-resubmit",
            now,
            Some(ConversationGuard {
                conversation_id,
                expected_home_epoch: before_edit.conversation.home_epoch,
                expected_conversation_seq: Some(
                    ConversationSeq::new(
                        before_edit
                            .conversation
                            .next_conversation_seq
                            .get()
                            .saturating_sub(1),
                    )
                    .expect("current sequence"),
                ),
            }),
        ),
        EditAndResubmitMessage {
            target_message_id: second_message_id,
            preserved_branch_id: edit_preserved_branch_id,
            replacement_branch_id: edit_replacement_branch_id,
            message: SubmitMessage {
                project_task_target: None,
                message_id: edited_message_id,
                participant_id,
                content: InputContent::InlineUtf8(
                    InlineInput::new("edited atomic replacement").expect("edited message"),
                ),
                attachment_refs: Vec::new(),
                requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
                requested_features: RequestedFeatures::default(),
                execution_scope: ExecutionScope::Tenant { tenant_id },
            },
        },
    )
    .expect("atomic edit command");
    let edit_group_id = edit_command.metadata.correlation_id;
    let edit_grant = signed_test_grant(actor, edited_run_id, now);
    let edited = application
        .edit_and_resubmit_message_authorized(
            edit_command.clone(),
            edit_grant.clone(),
            AdmissionSelection::web(BTreeSet::new()),
        )
        .await
        .expect("atomic edit and resubmit");
    assert_eq!(edited.disposition, CommandDisposition::Committed);
    assert_eq!(
        edited
            .last_conversation_seq
            .expect("atomic edit last sequence")
            .get()
            - edited
                .first_conversation_seq
                .expect("atomic edit first sequence")
                .get()
            + 1,
        6,
        "branch preservation, activation, and admission must be one six-record append"
    );
    assert_eq!(
        application
            .edit_and_resubmit_message_authorized(
                edit_command,
                edit_grant,
                AdmissionSelection::web(BTreeSet::new()),
            )
            .await
            .expect("duplicate atomic edit")
            .disposition,
        CommandDisposition::Duplicate
    );
    assert_eq!(
        sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT active_branch_id FROM conversation.context_branch_state
             WHERE tenant_id = $1 AND conversation_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("active edit replacement branch"),
        edit_replacement_branch_id.into_uuid()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_store.ledger_records
             WHERE tenant_id = $1 AND conversation_id = $2
               AND conversation_seq BETWEEN $3::numeric AND $4::numeric
               AND context_branch_id = $5",
        )
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(
            edited
                .first_conversation_seq
                .expect("atomic edit first sequence")
                .get()
                .saturating_add(3)
                .to_string(),
        )
        .bind(
            edited
                .last_conversation_seq
                .expect("atomic edit last sequence")
                .get()
                .to_string(),
        )
        .bind(edit_replacement_branch_id.into_uuid())
        .fetch_one(&pool)
        .await
        .expect("atomic edit admission branch tags"),
        3
    );
    let edited_projection = product_projection
        .get_conversation_projection(GetConversationProjection {
            actor,
            conversation_id,
            preferred_snapshot_id: None,
        })
        .await
        .expect("edited branch projection");
    assert!(
        edited_projection
            .messages
            .iter()
            .any(|message| message.message_id == edited_message_id)
    );
    assert!(
        edited_projection
            .messages
            .iter()
            .all(|message| message.message_id != second_message_id),
        "the preserved target leaf must not leak into the active replacement projection"
    );
    let after_second_admission = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation after atomic edit admission");
    assert_eq!(
        after_second_admission.active_context_branch_id,
        Some(edit_replacement_branch_id),
        "Conversation Query must expose the authoritative active Context branch"
    );
    let edit_branches = after_second_admission
        .context_branches
        .iter()
        .filter(|branch| branch.edit_group_id == Some(edit_group_id))
        .collect::<Vec<_>>();
    assert_eq!(edit_branches.len(), 2);
    assert_eq!(
        edit_branches
            .iter()
            .map(|branch| branch.origin)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            ContextBranchOrigin::EditPreserved,
            ContextBranchOrigin::EditReplacement,
        ])
    );
    assert!(
        edit_branches
            .iter()
            .all(|branch| branch.edit_target_message_id == Some(second_message_id)),
        "Product navigation must not infer the edited target from branch ordering"
    );
    let control_message_id = MessageId::generate();
    let control = CommandEnvelope::new(
        metadata(
            CommandId::generate(),
            actor,
            "steer-before-archive",
            now,
            Some(ConversationGuard {
                conversation_id,
                expected_home_epoch: after_second_admission.conversation.home_epoch,
                expected_conversation_seq: Some(
                    ConversationSeq::new(
                        after_second_admission
                            .conversation
                            .next_conversation_seq
                            .get()
                            .saturating_sub(1),
                    )
                    .expect("current sequence"),
                ),
            }),
        ),
        EnqueueRunControl {
            run_id: edited_run_id,
            message_id: control_message_id,
            participant_id,
            kind: RunControlKind::Steering,
            content: InputContent::InlineUtf8(
                InlineInput::new("steer the queued Run").expect("control message"),
            ),
            attachment_refs: Vec::new(),
        },
    )
    .expect("Run control command");
    assert_eq!(
        application
            .enqueue_run_control(control.clone())
            .await
            .expect("enqueue Run control")
            .disposition,
        CommandDisposition::Committed
    );
    assert_eq!(
        application
            .enqueue_run_control(control)
            .await
            .expect("duplicate Run control")
            .disposition,
        CommandDisposition::Duplicate
    );
    let control_state: (i16, String) = sqlx::query_as(
        "SELECT control.status, record.conversation_seq::text
         FROM runtime.loop_controls AS control
         JOIN event_store.ledger_records AS record
           ON record.record_id = control.source_record_id
         WHERE control.message_id = $1",
    )
    .bind(control_message_id.into_uuid())
    .fetch_one(&pool)
    .await
    .expect("read durable Run control");
    assert_eq!(control_state.0, 1);

    let while_edited_run_active = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation while edited Run is active");
    let branch_switch_while_running = application
        .activate_context_branch(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    actor,
                    "reject-branch-switch-while-run-active",
                    now,
                    Some(ConversationGuard {
                        conversation_id,
                        expected_home_epoch: while_edited_run_active.conversation.home_epoch,
                        expected_conversation_seq: Some(
                            ConversationSeq::new(
                                while_edited_run_active
                                    .conversation
                                    .next_conversation_seq
                                    .get()
                                    .saturating_sub(1),
                            )
                            .expect("current sequence"),
                        ),
                    }),
                ),
                ActivateConversationContextBranch {
                    branch_id: edit_preserved_branch_id,
                },
            )
            .expect("branch switch while active command"),
        )
        .await
        .expect_err("an active Run must block Context branch navigation");
    assert_eq!(
        branch_switch_while_running.code,
        ErrorCode::RunTransitionInvalid
    );

    application
        .cancel_run(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    actor,
                    "cancel-before-edit-branch-navigation",
                    now,
                    Some(ConversationGuard {
                        conversation_id,
                        expected_home_epoch: while_edited_run_active.conversation.home_epoch,
                        expected_conversation_seq: Some(
                            ConversationSeq::new(
                                while_edited_run_active
                                    .conversation
                                    .next_conversation_seq
                                    .get()
                                    .saturating_sub(1),
                            )
                            .expect("current sequence"),
                        ),
                    }),
                ),
                CancelRun {
                    run_id: edited_run_id,
                    reason: CancelReason::UserRequested,
                },
            )
            .expect("cancel edited Run command"),
        )
        .await
        .expect("cancel edited Run before branch navigation");

    let before_preserved_activation = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation before preserved branch activation");
    application
        .activate_context_branch(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    actor,
                    "activate-edit-preserved-branch",
                    now,
                    Some(ConversationGuard {
                        conversation_id,
                        expected_home_epoch: before_preserved_activation.conversation.home_epoch,
                        expected_conversation_seq: Some(
                            ConversationSeq::new(
                                before_preserved_activation
                                    .conversation
                                    .next_conversation_seq
                                    .get()
                                    .saturating_sub(1),
                            )
                            .expect("current sequence"),
                        ),
                    }),
                ),
                ActivateConversationContextBranch {
                    branch_id: edit_preserved_branch_id,
                },
            )
            .expect("activate preserved branch command"),
        )
        .await
        .expect("activate preserved edit branch");
    let preserved_projection = product_projection
        .get_conversation_projection(GetConversationProjection {
            actor,
            conversation_id,
            preferred_snapshot_id: None,
        })
        .await
        .expect("preserved edit branch projection");
    assert!(
        preserved_projection
            .messages
            .iter()
            .any(|message| message.message_id == second_message_id),
        "switching to the preserved edit branch must restore the original User Message"
    );
    assert!(
        preserved_projection
            .messages
            .iter()
            .all(|message| message.message_id != edited_message_id),
        "the replacement User Message must not leak into the preserved edit branch"
    );

    let before_replacement_activation = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation before replacement branch reactivation");
    application
        .activate_context_branch(
            CommandEnvelope::new(
                metadata(
                    CommandId::generate(),
                    actor,
                    "reactivate-edit-replacement-branch",
                    now,
                    Some(ConversationGuard {
                        conversation_id,
                        expected_home_epoch: before_replacement_activation.conversation.home_epoch,
                        expected_conversation_seq: Some(
                            ConversationSeq::new(
                                before_replacement_activation
                                    .conversation
                                    .next_conversation_seq
                                    .get()
                                    .saturating_sub(1),
                            )
                            .expect("current sequence"),
                        ),
                    }),
                ),
                ActivateConversationContextBranch {
                    branch_id: edit_replacement_branch_id,
                },
            )
            .expect("reactivate replacement branch command"),
        )
        .await
        .expect("reactivate replacement edit branch");
    let replacement_projection = product_projection
        .get_conversation_projection(GetConversationProjection {
            actor,
            conversation_id,
            preferred_snapshot_id: None,
        })
        .await
        .expect("replacement edit branch projection after reactivation");
    assert!(
        replacement_projection
            .messages
            .iter()
            .any(|message| message.message_id == edited_message_id),
        "switching back to the replacement edit branch must restore the edited User Message"
    );
    assert!(
        replacement_projection
            .messages
            .iter()
            .all(|message| message.message_id != second_message_id),
        "the original User Message must not leak into the replacement edit branch"
    );

    let before_archive_submit = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation before archive-test admission");
    let archive_submit_id = CommandId::generate();
    let archive_run_id = derive_admission_run_id(archive_submit_id).expect("archive-test Run ID");
    application
        .submit_message_authorized(
            CommandEnvelope::new(
                metadata(
                    archive_submit_id,
                    actor,
                    "submit-active-run-for-archive",
                    now,
                    Some(ConversationGuard {
                        conversation_id,
                        expected_home_epoch: before_archive_submit.conversation.home_epoch,
                        expected_conversation_seq: Some(
                            ConversationSeq::new(
                                before_archive_submit
                                    .conversation
                                    .next_conversation_seq
                                    .get()
                                    .saturating_sub(1),
                            )
                            .expect("current sequence"),
                        ),
                    }),
                ),
                SubmitMessage {
                    project_task_target: None,
                    message_id: MessageId::generate(),
                    participant_id,
                    content: InputContent::InlineUtf8(
                        InlineInput::new("archive this replacement-branch Run")
                            .expect("archive-test message"),
                    ),
                    attachment_refs: Vec::new(),
                    requested_model_class: ModelClass::new("chat.balanced").expect("model class"),
                    requested_features: RequestedFeatures::default(),
                    execution_scope: ExecutionScope::Tenant { tenant_id },
                },
            )
            .expect("archive-test submit command"),
            signed_test_grant(actor, archive_run_id, now),
            AdmissionSelection::web(BTreeSet::new()),
        )
        .await
        .expect("admit archive-test Run");

    let before_archive = application
        .get_conversation(GetConversation {
            actor,
            conversation_id,
        })
        .await
        .expect("Conversation before archive");
    let archive_metadata = |idempotency: &str| {
        metadata(
            CommandId::generate(),
            actor,
            idempotency,
            now,
            Some(ConversationGuard {
                conversation_id,
                expected_home_epoch: before_archive.conversation.home_epoch,
                expected_conversation_seq: Some(
                    ConversationSeq::new(
                        before_archive
                            .conversation
                            .next_conversation_seq
                            .get()
                            .saturating_sub(1),
                    )
                    .expect("current sequence"),
                ),
            }),
        )
    };
    let rejected = application
        .archive_conversation(
            CommandEnvelope::new(
                archive_metadata("archive-reject-active"),
                ArchiveConversation {
                    active_run_policy: ActiveRunArchivePolicy::Reject,
                },
            )
            .expect("reject archive command"),
        )
        .await
        .expect_err("active Run must block Reject policy");
    assert_eq!(rejected.code, ErrorCode::RunTransitionInvalid);
    let archive = CommandEnvelope::new(
        archive_metadata("archive-cancel-active"),
        ArchiveConversation {
            active_run_policy: ActiveRunArchivePolicy::CancelThenArchive,
        },
    )
    .expect("archive command");
    assert_eq!(
        application
            .archive_conversation(archive.clone())
            .await
            .expect("archive with cancellation")
            .disposition,
        CommandDisposition::Committed
    );
    assert_eq!(
        application
            .archive_conversation(archive)
            .await
            .expect("duplicate archive")
            .disposition,
        CommandDisposition::Duplicate
    );
    assert_eq!(
        application
            .get_conversation(GetConversation {
                actor,
                conversation_id,
            })
            .await
            .expect("archived Conversation")
            .conversation
            .lifecycle,
        ConversationLifecycle::Archived
    );
    assert_eq!(
        application
            .get_run(musteragent_run::GetRun {
                actor,
                run_id: archive_run_id,
            })
            .await
            .expect("Run cancelled by archive")
            .run
            .state
            .status(),
        RunStatus::Cancelled
    );

    // Unarchive: guarded like archive, idempotent, and reversible again.
    let guarded_metadata =
        |idempotency: &str, view: &musteragent_conversation::ConversationView| {
            metadata(
                CommandId::generate(),
                actor,
                idempotency,
                now,
                Some(ConversationGuard {
                    conversation_id,
                    expected_home_epoch: view.conversation.home_epoch,
                    expected_conversation_seq: Some(
                        ConversationSeq::new(
                            view.conversation
                                .next_conversation_seq
                                .get()
                                .saturating_sub(1),
                        )
                        .expect("current sequence"),
                    ),
                }),
            )
        };
    let current = || async {
        application
            .get_conversation(GetConversation {
                actor,
                conversation_id,
            })
            .await
            .expect("current Conversation")
    };
    let archived = current().await;
    let stranger = Actor {
        tenant_id,
        principal_id: PrincipalId::generate(),
        principal_kind: PrincipalKind::User,
    };
    let mut stranger_metadata = guarded_metadata("unarchive-stranger", &archived);
    stranger_metadata.actor = stranger;
    let denied = application
        .unarchive_conversation(
            CommandEnvelope::new(stranger_metadata, UnarchiveConversation {})
                .expect("stranger unarchive command"),
        )
        .await
        .expect_err("a non-participant cannot unarchive");
    assert_eq!(denied.code, ErrorCode::ConversationNotFound);
    assert_eq!(
        current().await.conversation.lifecycle,
        ConversationLifecycle::Archived
    );

    let unarchive = CommandEnvelope::new(
        guarded_metadata("unarchive-restore", &archived),
        UnarchiveConversation {},
    )
    .expect("unarchive command");
    let restored = application
        .unarchive_conversation(unarchive.clone())
        .await
        .expect("unarchive archived Conversation");
    assert_eq!(restored.disposition, CommandDisposition::Committed);
    assert_eq!(
        restored.first_conversation_seq,
        restored.last_conversation_seq
    );
    let replayed = application
        .unarchive_conversation(unarchive)
        .await
        .expect("replayed unarchive");
    assert_eq!(replayed.disposition, CommandDisposition::Duplicate);
    assert_eq!(
        replayed.last_conversation_seq,
        restored.last_conversation_seq
    );
    let active = current().await;
    assert_eq!(active.conversation.lifecycle, ConversationLifecycle::Active);

    // A fresh unarchive of an already active Conversation records nothing.
    let already_active = application
        .unarchive_conversation(
            CommandEnvelope::new(
                guarded_metadata("unarchive-already-active", &active),
                UnarchiveConversation {},
            )
            .expect("unarchive active command"),
        )
        .await
        .expect("unarchive of an active Conversation");
    assert_eq!(already_active.disposition, CommandDisposition::Duplicate);
    assert_eq!(
        current().await.conversation.next_conversation_seq,
        active.conversation.next_conversation_seq
    );

    // The restored Conversation can be archived again.
    assert_eq!(
        application
            .archive_conversation(
                CommandEnvelope::new(
                    guarded_metadata("archive-after-unarchive", &active),
                    ArchiveConversation {
                        active_run_policy: ActiveRunArchivePolicy::Reject,
                    },
                )
                .expect("re-archive command"),
            )
            .await
            .expect("archive after unarchive")
            .disposition,
        CommandDisposition::Committed
    );
    assert_eq!(
        current().await.conversation.lifecycle,
        ConversationLifecycle::Archived
    );
}

#[path = "postgres_runtime_application/coordination.rs"]
mod coordination;

#[path = "postgres_runtime_application/private_execution.rs"]
mod private_execution;

#[path = "postgres_runtime_application/shared_admission.rs"]
mod shared_admission;
