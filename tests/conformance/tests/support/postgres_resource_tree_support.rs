#![allow(
    dead_code,
    reason = "Integration test binaries use different subsets of this shared fixture."
)]

use musteragent_artifact::ArtifactSource;
use musteragent_artifact::ArtifactVersionAuthor;
// This support module is compiled into three independent integration-test
// binaries. Each binary intentionally exercises a different subset of these
// shared fixture builders.
use std::sync::Arc;

use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactVersion,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    Actor, ArtifactId, ArtifactVersionId, CapabilityProviderId, ConversationId, IdempotencyKey,
    InvocationId, ParticipantId, PrincipalId, PrincipalKind, ResourceChangeSetId, ResourceNodeId,
    TenantId, ToolCallId, UtcTimestamp, VersionNumber, VersionedKey, WorkspaceId,
};
use musteragent_policy::{ApprovalRequirement, Effect};
use musteragent_postgres::PostgresFencedToolInvocationJournal;
use musteragent_resource::ResourceName;
use musteragent_runtime_capability::{
    FencedToolInvocationJournal, ToolExecutionStart, ToolStartMode,
};
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_resource::{
    ResourceChangeSetMutation, ResourceCommandContext, ResourceMutationIdentity,
};
use musteragent_tool::{ToolInvocation, ToolInvocationStatus, ToolKey, ToolRetryContract};

use crate::running_loop;

pub(crate) async fn start_resource_tool(
    pool: &sqlx::PgPool,
    content: &Arc<dyn ContentStore>,
    journal: &PostgresFencedToolInvocationJournal,
    attempt: &musteragent_run::AttemptContext,
    fixture: running_loop::Fixture,
    suffix: &str,
) -> InvocationId {
    start_resource_tool_with_contract(
        pool,
        content,
        journal,
        attempt,
        fixture,
        suffix,
        "resource.folder.create@1",
        format!(r#"{{"name":"{suffix}","expected_revision":0}}"#).into_bytes(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn start_resource_tool_with_contract(
    pool: &sqlx::PgPool,
    content: &Arc<dyn ContentStore>,
    journal: &PostgresFencedToolInvocationJournal,
    attempt: &musteragent_run::AttemptContext,
    fixture: running_loop::Fixture,
    suffix: &str,
    tool_key: &str,
    request: Vec<u8>,
) -> InvocationId {
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("JSON media type"),
            bytes: request,
        })
        .await
        .expect("persist Resource Tool request");
    let invocation_id = InvocationId::generate();
    journal
        .load_or_begin(
            attempt,
            ToolInvocation {
                invocation_id,
                run_id: fixture.run_id,
                requested_by_attempt_id: attempt.attempt_id,
                execution_attempt_id: None,
                tool_call_id: ToolCallId::new(format!("call-resource-{suffix}"))
                    .expect("Tool call ID"),
                tool_key: ToolKey::from_versioned_key(
                    VersionedKey::new(tool_key).expect("Tool key"),
                ),
                capability_provider_id: CapabilityProviderId::generate(),
                status: ToolInvocationStatus::Requested,
                effect: Effect::WriteReversible,
                approval: ApprovalRequirement::Never,
                idempotency_key: IdempotencyKey::new(format!("tool:resource:{suffix}"))
                    .expect("Tool idempotency"),
                request_ref,
                result_ref: None,
                interaction_id: None,
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
            },
            ToolRetryContract::IdempotentByKey,
        )
        .await
        .expect("begin Resource Tool");
    journal
        .start(
            attempt,
            invocation_id,
            ToolStartMode::Initial,
            ToolExecutionStart {
                started_at: running_loop::database_now(pool).await,
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
            },
        )
        .await
        .expect("start Resource Tool");
    invocation_id
}

pub(crate) async fn start_file_edit_tool(
    pool: &sqlx::PgPool,
    content: &Arc<dyn ContentStore>,
    journal: &PostgresFencedToolInvocationJournal,
    attempt: &musteragent_run::AttemptContext,
    fixture: running_loop::Fixture,
    file_node_id: ResourceNodeId,
) -> InvocationId {
    let request_ref = content
        .put(PutContent {
            tenant_id: fixture.tenant_id,
            media_type: MediaType::new("application/json").expect("JSON media type"),
            bytes: format!(r#"{{"file_node_id":"{file_node_id}","base_version":1}}"#).into_bytes(),
        })
        .await
        .expect("persist File Edit Tool request");
    let invocation_id = InvocationId::generate();
    journal
        .load_or_begin(
            attempt,
            ToolInvocation {
                invocation_id,
                run_id: fixture.run_id,
                requested_by_attempt_id: attempt.attempt_id,
                execution_attempt_id: None,
                tool_call_id: ToolCallId::new("call-file-edit-atomic").expect("Tool call ID"),
                tool_key: ToolKey::from_versioned_key(
                    VersionedKey::new("resource.file.edit.begin@1").expect("Tool key"),
                ),
                capability_provider_id: CapabilityProviderId::generate(),
                status: ToolInvocationStatus::Requested,
                effect: Effect::WriteReversible,
                approval: ApprovalRequirement::Never,
                idempotency_key: IdempotencyKey::new("tool:file-edit:atomic")
                    .expect("Tool idempotency"),
                request_ref,
                result_ref: None,
                interaction_id: None,
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
            },
            ToolRetryContract::IdempotentByKey,
        )
        .await
        .expect("begin File Edit Tool");
    journal
        .start(
            attempt,
            invocation_id,
            ToolStartMode::Initial,
            ToolExecutionStart {
                started_at: running_loop::database_now(pool).await,
                timing: musteragent_runtime_contract::execution_liveness::InvocationExecutionTiming::LegacyFixed { deadline: UtcTimestamp::from_unix_millis(i64::MAX) },
            },
        )
        .await
        .expect("start File Edit Tool");
    invocation_id
}

pub(crate) fn tool_context(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    conversation_id: ConversationId,
    workspace_id: WorkspaceId,
    invocation_id: InvocationId,
    now: UtcTimestamp,
) -> ResourceCommandContext {
    ResourceCommandContext {
        tenant_id,
        actor: Actor {
            tenant_id,
            principal_id,
            principal_kind: PrincipalKind::User,
        },
        source: musteragent_runtime_resource::ResourceCommandSource::Conversation {
            conversation_id,
            workspace_id: Some(workspace_id),
        },
        invocation_id: Some(invocation_id),
        tool_key: Some(VersionedKey::new("resource.folder.create@1").expect("Resource Tool key")),
        now,
    }
}

pub(crate) fn file_edit_tool_context(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    conversation_id: ConversationId,
    workspace_id: WorkspaceId,
    invocation_id: InvocationId,
    now: UtcTimestamp,
) -> ResourceCommandContext {
    ResourceCommandContext {
        tenant_id,
        actor: Actor {
            tenant_id,
            principal_id,
            principal_kind: PrincipalKind::User,
        },
        source: musteragent_runtime_resource::ResourceCommandSource::Conversation {
            conversation_id,
            workspace_id: Some(workspace_id),
        },
        invocation_id: Some(invocation_id),
        tool_key: Some(
            VersionedKey::new("resource.file.edit.begin@1").expect("File Edit Tool key"),
        ),
        now,
    }
}

pub(crate) fn context(
    tenant_id: TenantId,
    principal_id: PrincipalId,
    conversation_id: ConversationId,
    workspace_id: WorkspaceId,
    now_ms: i64,
) -> ResourceCommandContext {
    ResourceCommandContext {
        tenant_id,
        actor: Actor {
            tenant_id,
            principal_id,
            principal_kind: PrincipalKind::User,
        },
        source: musteragent_runtime_resource::ResourceCommandSource::Conversation {
            conversation_id,
            workspace_id: Some(workspace_id),
        },
        invocation_id: None,
        tool_key: None,
        now: UtcTimestamp::from_unix_millis(now_ms),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn product_file_mutation(
    mutation_key: &str,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    participant_id: ParticipantId,
    node_id: ResourceNodeId,
    parent_node_id: ResourceNodeId,
    file_name: &str,
    artifact_id: ArtifactId,
    content_ref: ContentRef,
    now: UtcTimestamp,
) -> ResourceChangeSetMutation {
    ResourceChangeSetMutation::CreateFile {
        mutation_key: idempotency(mutation_key),
        node_id,
        parent_node_id: Some(parent_node_id),
        name: name(file_name),
        sort_key: file_name.to_owned(),
        artifact: Artifact {
            artifact_id,
            tenant_id,
            source: ArtifactSource::Conversation(conversation_id),
            origin: ArtifactOrigin::Product,
            kind: ArtifactKind::File,
            lifecycle: ArtifactLifecycle::Draft,
            current_version: VersionNumber::new(1).expect("first version"),
            title: file_name.to_owned(),
            created_at: now,
            updated_at: now,
        },
        version: Box::new(ArtifactVersion {
            artifact_version_id: ArtifactVersionId::generate(),
            artifact_id,
            version: VersionNumber::new(1).expect("first version"),
            content_hash: content_ref.sha256,
            content_ref,
            dependency_refs: Vec::new(),
            source_invocation_id: None,
            committed_by: ArtifactVersionAuthor::Participant(participant_id),
            committed_at: now,
        }),
    }
}

pub(crate) fn idempotency(value: &str) -> IdempotencyKey {
    IdempotencyKey::new(format!("change-set:{value}")).expect("valid mutation key")
}

pub(crate) fn mutation(label: &str) -> ResourceMutationIdentity {
    ResourceMutationIdentity {
        change_set_id: ResourceChangeSetId::generate(),
        idempotency_key: IdempotencyKey::new(format!("resource:{label}"))
            .expect("valid idempotency key"),
        command_fingerprint: Sha256::digest(label.as_bytes()),
    }
}

pub(crate) fn name(value: &str) -> ResourceName {
    ResourceName::new(value).expect("valid resource name")
}

pub(crate) async fn seed_project_conversations(
    pool: &sqlx::PgPool,
    tenant_id: TenantId,
    principal_id: PrincipalId,
    workspace_id: WorkspaceId,
    conversation_ids: [ConversationId; 2],
) {
    let mut transaction = pool.begin().await.expect("begin Project fixture");
    for conversation_id in conversation_ids {
        let owner_id = ParticipantId::generate();
        sqlx::query(
            "INSERT INTO conversation.conversations (
                 conversation_id, tenant_id, kind, title, control_home_kind,
                 control_device_id, home_epoch, lifecycle, created_by,
                 created_at_ms, updated_at_ms, archived_at_ms,
                 workspace_id, execution_owner_participant_id
             ) VALUES ($1, $2, 1, 'Resource Project', 1, NULL, 1, 1, $3,
                       1, 1, NULL, $4, $5)",
        )
        .bind(conversation_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(principal_id.into_uuid())
        .bind(workspace_id.into_uuid())
        .bind(owner_id.into_uuid())
        .execute(&mut *transaction)
        .await
        .expect("seed Project Conversation");
        sqlx::query(
            "INSERT INTO conversation.participants (
                 participant_id, tenant_id, conversation_id, kind, principal_ref,
                 agent_ref, service_ref, display_name, avatar_content_id,
                 joined_at_ms, left_at_ms
             ) VALUES (
                 $1, $2, $3, 2, NULL,
                 (SELECT agent_id FROM agent.agent_definitions WHERE agent_key = 'assistant'),
                 NULL, 'Kernel', NULL, 1, NULL
             )",
        )
        .bind(owner_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .execute(&mut *transaction)
        .await
        .expect("seed execution owner");
        // A readable conversation has the user membership established by the
        // production create-conversation owner, independently of its Agent.
        sqlx::query(
            "INSERT INTO conversation.participants (
                 participant_id, tenant_id, conversation_id, kind, principal_ref,
                 agent_ref, service_ref, display_name, avatar_content_id,
                 joined_at_ms, left_at_ms
             ) VALUES ($1, $2, $3, 1, $4, NULL, NULL, 'User', NULL, 1, NULL)",
        )
        .bind(ParticipantId::generate().into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(conversation_id.into_uuid())
        .bind(principal_id.into_uuid())
        .execute(&mut *transaction)
        .await
        .expect("seed original user membership");
    }
    transaction.commit().await.expect("commit Project fixture");
}
