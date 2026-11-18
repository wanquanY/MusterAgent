mod account;
mod committed_events;
pub(crate) mod device_export_identity;
pub(crate) mod dialogue_artifact;
mod dialogue_device;
mod sharing;
pub(crate) mod task_result;
mod work_item_erasure;
pub(crate) use work_item_erasure::{collect_work_item_artifacts, erase_work_item_artifacts};
mod agent_directory;
mod resource_projection_recovery;
pub(crate) use committed_events::invocation_events;
use musteragent_artifact::ArtifactSource;
use musteragent_artifact::ArtifactVersionAuthor;
use std::collections::BTreeMap;
use std::sync::Arc;

use musteragent_artifact::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactVersion,
};
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_identity::{
    ActivityId, Actor, ArtifactId, ArtifactVersionId, ConversationId, CorrelationId, InvocationId,
    ParticipantId, RunId, TenantId, UtcTimestamp, VersionNumber,
};
use musteragent_ledger::{AppendReceipt, LedgerPayloadEncoder};
use musteragent_runtime_artifact::{
    ArtifactApplication, ArtifactCommitDisposition, ArtifactCommitReceipt, ArtifactMutation,
    ArtifactPage, ArtifactReadFailure, ArtifactRuntimeError, ArtifactStore, ArtifactSummary,
    ArtifactView, ClassifyArtifactReadError, CommitToolArtifact, GetArtifact, ListArtifacts,
    ListConversationArtifacts,
};
use musteragent_runtime_content::ArtifactContentAuthorization;
use musteragent_runtime_contract::runtime_error;
use musteragent_runtime_contract::{ErrorCode, RuntimeError};
use musteragent_storage_protection::{OpaqueRefKeyring, OpaqueRefProtectionError};
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use thiserror::Error;

use crate::ledger::{BeginAppend, PostgresLedgerError, PostgresLedgerStore};

#[path = "artifact/support.rs"]
mod artifact_support;
mod independent_files;
#[path = "artifact/rows.rs"]
mod rows;
#[path = "artifact/version_content.rs"]
mod version_content;
#[path = "artifact/workspace.rs"]
mod workspace;
use artifact_support::*;
pub(crate) use rows::decode_version;
use rows::{decode_artifact, decode_summary};

const TOOL_RUNNING: i16 = 3;
const TOOL_SUCCEEDED: i16 = 4;
const CONTENT_READY: i16 = 2;

fn decode_content_reachability(
    rows: Vec<PgRow>,
    references: &[ContentRef],
    correlation_id: CorrelationId,
) -> Result<Vec<bool>, RuntimeError> {
    let mut reachable = BTreeMap::new();
    for row in rows {
        let decoded = (
            row.try_get::<String, _>("media_type"),
            row.try_get::<String, _>("byte_length"),
            row.try_get::<Vec<u8>, _>("content_hash"),
        );
        let (Ok(media_type), Ok(byte_length), Ok(content_hash)) = decoded else {
            return Err(runtime_error(
                ErrorCode::InternalInvariantViolated,
                correlation_id,
                false,
                "Artifact authorization content identity is invalid",
            ));
        };
        let content_id = row.try_get::<uuid::Uuid, _>("content_id").map_err(|_| {
            runtime_error(
                ErrorCode::InternalInvariantViolated,
                correlation_id,
                false,
                "Artifact authorization content identity is invalid",
            )
        })?;
        reachable.insert(content_id, (media_type, byte_length, content_hash));
    }
    Ok(references
        .iter()
        .map(|reference| {
            reachable
                .get(&reference.content_id.into_uuid())
                .is_some_and(|(media_type, byte_length, content_hash)| {
                    media_type == reference.media_type.as_str()
                        && byte_length == &reference.byte_length.to_string()
                        && content_hash.as_slice() == reference.sha256.into_bytes().as_slice()
                })
        })
        .collect())
}

#[derive(Clone)]
pub struct PostgresArtifactStore {
    ledger: PostgresLedgerStore,
    reference_keys: Arc<OpaqueRefKeyring>,
    payload_encoder: Option<Arc<dyn LedgerPayloadEncoder>>,
}

impl PostgresArtifactStore {
    #[must_use]
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        reference_keys: Arc<OpaqueRefKeyring>,
    ) -> Self {
        Self {
            ledger: PostgresLedgerStore::new(database),
            reference_keys,
            payload_encoder: None,
        }
    }

    #[must_use]
    pub fn with_payload_encoder(mut self, encoder: Arc<dyn LedgerPayloadEncoder>) -> Self {
        self.payload_encoder = Some(encoder);
        self
    }

    async fn commit_inner(
        &self,
        command: CommitToolArtifact,
    ) -> Result<ArtifactCommitReceipt, PostgresArtifactError> {
        command.validate()?;
        // This historical API carries no Attempt fence or Home supervision
        // authority. Activity executions must use the fenced effect committer.
        let legacy: Option<bool> = sqlx::query_scalar(
            "SELECT activity_timing IS NULL FROM tool.tool_invocations
             WHERE tenant_id = $1 AND invocation_id = $2",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.invocation_id.into_uuid())
        .fetch_optional(self.ledger.database().pool())
        .await?;
        if legacy != Some(true) {
            return Err(PostgresArtifactError::ToolTransitionConflict);
        }
        let artifact_id = command.mutation.artifact_id();
        let version = command.mutation.version().version;
        let mut prepared = match self.ledger.begin_append(&command.append).await? {
            BeginAppend::Duplicate(ledger) => {
                self.verify_duplicate(&command, &ledger).await?;
                return Ok(ArtifactCommitReceipt {
                    disposition: ArtifactCommitDisposition::Duplicate,
                    artifact_id,
                    version,
                    ledger,
                });
            }
            BeginAppend::Ready(prepared) => prepared,
        };
        let allocated_sequences = prepared.sequences().to_vec();
        let transaction = prepared.transaction_mut();
        authorize_content(transaction, command.tenant_id, command.mutation.version()).await?;
        authorize_result_content(transaction, command.tenant_id, &command.result).await?;
        let invocation = lock_invocation(transaction, &command).await?;

        match &command.mutation {
            ArtifactMutation::Create { artifact, version } => {
                if artifact.created_by_run_id() != Some(invocation.run_id)
                    || Some(invocation.conversation_id) != artifact.source.conversation_id()
                {
                    return Err(PostgresArtifactError::IdentityMismatch);
                }
                insert_artifact(transaction, artifact).await?;
                insert_version(transaction, version).await?;
            }
            ArtifactMutation::Update {
                artifact_id,
                expected_version,
                version,
                title,
            } => {
                update_artifact(
                    transaction,
                    command.tenant_id,
                    invocation.conversation_id,
                    *artifact_id,
                    *expected_version,
                    version,
                    title.as_deref(),
                )
                .await?;
                insert_version(transaction, version).await?;
            }
        }
        complete_tool(
            transaction,
            &self.reference_keys,
            command.tenant_id,
            command.invocation_id,
            &command.result,
            command.mutation.version().committed_at,
            false,
        )
        .await?;
        insert_commit_command(transaction, &command, &allocated_sequences).await?;
        let ledger = prepared.commit(&command.append).await?;
        Ok(ArtifactCommitReceipt {
            disposition: ArtifactCommitDisposition::Committed,
            artifact_id,
            version,
            ledger,
        })
    }

    async fn verify_duplicate(
        &self,
        command: &CommitToolArtifact,
        receipt: &AppendReceipt,
    ) -> Result<(), PostgresArtifactError> {
        let row = sqlx::query(
            "SELECT command_fingerprint, invocation_id, artifact_id,
                    artifact_version_id,
                    ledger_first_sequence::text AS ledger_first_sequence,
                    ledger_last_sequence::text AS ledger_last_sequence
             FROM artifact.commit_commands
             WHERE tenant_id = $1 AND idempotency_key = $2",
        )
        .bind(command.tenant_id.into_uuid())
        .bind(command.append.idempotency_key.as_str())
        .fetch_optional(self.ledger.pool())
        .await?
        .ok_or(PostgresArtifactError::IdempotencyConflict)?;
        let stored_dependencies =
            load_version_dependencies(self.ledger.pool(), vec![command.mutation.version().clone()])
                .await?
                .pop()
                .ok_or(PostgresArtifactError::Corrupt)?
                .dependency_refs;
        if row.try_get::<Vec<u8>, _>("command_fingerprint")?
            != command.append.command_fingerprint.into_bytes()
            || row.try_get::<uuid::Uuid, _>("invocation_id")? != command.invocation_id.into_uuid()
            || row.try_get::<uuid::Uuid, _>("artifact_id")?
                != command.mutation.artifact_id().into_uuid()
            || row.try_get::<uuid::Uuid, _>("artifact_version_id")?
                != command.mutation.version().artifact_version_id.into_uuid()
            || parse_u64(row.try_get("ledger_first_sequence")?)? != receipt.first_sequence.get()
            || parse_u64(row.try_get("ledger_last_sequence")?)? != receipt.last_sequence.get()
            || stored_dependencies != command.mutation.version().dependency_refs
        {
            return Err(PostgresArtifactError::IdempotencyConflict);
        }
        Ok(())
    }

    async fn load_inner(
        &self,
        tenant_id: TenantId,
        artifact_id: ArtifactId,
    ) -> Result<ArtifactView, PostgresArtifactError> {
        let mut tx = self
            .ledger
            .database()
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Resource,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await?;
        let view = self.load_on(&mut tx, tenant_id, artifact_id).await?;
        tx.commit().await?;
        Ok(view)
    }

    async fn load_on(
        &self,
        connection: &mut sqlx::PgConnection,
        tenant_id: TenantId,
        artifact_id: ArtifactId,
    ) -> Result<ArtifactView, PostgresArtifactError> {
        let row = sqlx::query(
            "SELECT artifact.artifact_id, artifact.tenant_id,
                    artifact.conversation_id, artifact.workspace_id, artifact.activity_id,
                    artifact.created_by_run_id, artifact.origin, artifact.kind,
                    artifact.lifecycle,
                    artifact.current_version::text AS current_version,
                    artifact.title, artifact.created_at_ms, artifact.updated_at_ms
             FROM artifact.artifacts AS artifact
             WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(artifact_id.into_uuid())
        .fetch_optional(&mut *connection)
        .await?
        .ok_or(PostgresArtifactError::NotFound)?;
        let artifact = decode_artifact(&row)?;
        let rows = sqlx::query(
            "SELECT version.artifact_version_id, version.artifact_id,
                    version.version::text AS version, version.content_id,
                    version.content_hash, version.source_invocation_id,
                    version.committed_by, version.committed_by_principal_id, version.committed_at_ms,
                    content.media_type,
                    content.byte_length::text AS byte_length,
                    content.content_hash AS ref_hash
             FROM artifact.artifact_versions AS version
             JOIN artifact.content_refs AS content
               ON content.content_id = version.content_id
             WHERE version.artifact_id = $1 AND content.tenant_id = $2
               AND content.state = $3
             ORDER BY version.version ASC",
        )
        .bind(artifact_id.into_uuid())
        .bind(tenant_id.into_uuid())
        .bind(CONTENT_READY)
        .fetch_all(&mut *connection)
        .await?;
        let versions = rows
            .into_iter()
            .map(decode_version)
            .collect::<Result<Vec<_>, _>>()?;
        let versions = load_version_dependencies(&mut *connection, versions).await?;
        if versions.last().map(|value| value.version) != Some(artifact.current_version) {
            return Err(PostgresArtifactError::Corrupt);
        }
        Ok(ArtifactView { artifact, versions })
    }

    async fn list_inner(
        &self,
        query: ListArtifacts,
    ) -> Result<Vec<Artifact>, PostgresArtifactError> {
        query.validate()?;
        let rows = sqlx::query(
            "SELECT artifact_id, tenant_id, conversation_id, workspace_id, activity_id,
                    created_by_run_id, origin, kind, lifecycle,
                    current_version::text AS current_version,
                    title, created_at_ms, updated_at_ms
             FROM artifact.artifacts
             WHERE tenant_id = $1 AND conversation_id = $2
               AND ($3::uuid IS NULL OR artifact_id < $3)
             ORDER BY artifact_id DESC
             LIMIT $4",
        )
        .bind(query.tenant_id.into_uuid())
        .bind(query.conversation_id.into_uuid())
        .bind(query.before_artifact_id.map(ArtifactId::into_uuid))
        .bind(i64::try_from(query.limit).map_err(|_| ArtifactRuntimeError::InvalidPageSize)?)
        .fetch_all(self.ledger.pool())
        .await?;
        rows.into_iter().map(|row| decode_artifact(&row)).collect()
    }

    async fn get_public_inner(&self, query: GetArtifact) -> Result<ArtifactView, RuntimeError> {
        let correlation_id = musteragent_identity::CorrelationId::generate();
        let resolved_artifact_id = query.artifact_id;
        let authorized: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM artifact.artifacts AS artifact
                 WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $3
                   AND EXISTS (SELECT 1 FROM conversation.conversations public_context
                       WHERE public_context.tenant_id=artifact.tenant_id
                         AND public_context.conversation_id=artifact.conversation_id
                         AND public_context.purpose='standard')
                   AND (
                       EXISTS (
                           SELECT 1 FROM conversation.participants AS viewer
                           WHERE viewer.tenant_id = artifact.tenant_id
                             AND viewer.conversation_id = artifact.conversation_id
                             AND viewer.kind = 1
                             AND viewer.principal_ref = $2
                             AND viewer.left_at_ms IS NULL
                       )
                   )
             )",
        )
        .bind(query.actor.tenant_id.into_uuid())
        .bind(query.actor.principal_id.into_uuid())
        .bind(resolved_artifact_id.into_uuid())
        .fetch_one(self.ledger.pool())
        .await
        .map_err(|_| {
            runtime_error(
                ErrorCode::DependencyUnavailable,
                correlation_id,
                true,
                "Artifact storage is unavailable",
            )
        })?;
        if !authorized {
            return Err(runtime_error(
                ErrorCode::ArtifactNotFound,
                correlation_id,
                false,
                "Artifact was not found",
            ));
        }
        self.load_inner(query.actor.tenant_id, resolved_artifact_id)
            .await
            .map_err(|error| map_public_query_error(error, correlation_id))
    }

    async fn list_public_inner(
        &self,
        query: ListConversationArtifacts,
    ) -> Result<ArtifactPage, RuntimeError> {
        let correlation_id = musteragent_identity::CorrelationId::generate();
        query.validate().map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Artifact page size is invalid",
            )
        })?;
        let fetch_limit = query.limit.checked_add(1).ok_or_else(|| {
            runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Artifact page size is invalid",
            )
        })?;
        let rows = sqlx::query(
            "SELECT artifact.artifact_id, artifact.tenant_id,
                    artifact.conversation_id, artifact.workspace_id, artifact.activity_id,
                    artifact.created_by_run_id, artifact.origin,
                    artifact.kind, artifact.lifecycle,
                    artifact.current_version::text AS current_version,
                    artifact.title, artifact.created_at_ms, artifact.updated_at_ms,
                    version.artifact_version_id AS current_artifact_version_id,
                    version.version::text AS current_version_number,
                    version.content_id AS current_content_id,
                    version.content_hash AS current_content_hash,
                    version.source_invocation_id AS current_source_invocation_id,
                    version.committed_by AS current_committed_by,
                    version.committed_by_principal_id AS current_committed_by_principal_id,
                    version.committed_at_ms AS current_committed_at_ms,
                    content.media_type AS current_media_type,
                    content.byte_length::text AS current_byte_length,
                    content.content_hash AS current_ref_hash
             FROM artifact.artifacts AS artifact
             LEFT JOIN conversation.participants AS viewer
               ON viewer.tenant_id = artifact.tenant_id
              AND viewer.conversation_id = artifact.conversation_id
              AND viewer.kind = 1
              AND viewer.principal_ref = $3
              AND viewer.left_at_ms IS NULL
             JOIN artifact.artifact_versions AS version
               ON version.artifact_id = artifact.artifact_id
              AND version.version = artifact.current_version
             JOIN artifact.content_refs AS content
               ON content.content_id = version.content_id
              AND content.tenant_id = artifact.tenant_id
              AND content.state = $5
             WHERE artifact.tenant_id = $1
               AND artifact.conversation_id = $2
               AND EXISTS (SELECT 1 FROM conversation.conversations public_context
                 WHERE public_context.tenant_id=artifact.tenant_id
                   AND public_context.conversation_id=artifact.conversation_id
                   AND public_context.purpose='standard')
               AND viewer.participant_id IS NOT NULL
               AND ($4::uuid IS NULL OR artifact.artifact_id < $4)
               AND (
                   $6::text IS NULL
                   OR NOT EXISTS (
                       SELECT 1
                       FROM unnest(regexp_split_to_array(lower(trim($6)), '\\s+')) AS token
                       WHERE artifact.search_text NOT LIKE '%' || token || '%'
                   )
               )
               AND (cardinality($7::smallint[]) = 0 OR artifact.kind = ANY($7))
             ORDER BY artifact.artifact_id DESC
             LIMIT $8",
        )
        .bind(query.actor.tenant_id.into_uuid())
        .bind(query.conversation_id.into_uuid())
        .bind(query.actor.principal_id.into_uuid())
        .bind(query.before_artifact_id.map(ArtifactId::into_uuid))
        .bind(CONTENT_READY)
        .bind(query.search_query.as_deref().map(str::trim))
        .bind(
            query
                .kinds
                .iter()
                .copied()
                .map(kind_code)
                .collect::<Vec<_>>(),
        )
        .bind(i64::try_from(fetch_limit).map_err(|_| {
            runtime_error(
                ErrorCode::InvalidCommand,
                correlation_id,
                false,
                "Artifact page size is invalid",
            )
        })?)
        .fetch_all(self.ledger.pool())
        .await
        .map_err(|_| {
            runtime_error(
                ErrorCode::DependencyUnavailable,
                correlation_id,
                true,
                "Artifact storage is unavailable",
            )
        })?;
        let has_more = rows.len() > query.limit;
        let mut artifacts = rows
            .into_iter()
            .take(query.limit)
            .map(|row| decode_summary(&row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| map_public_query_error(error, correlation_id))?;
        let current_versions = load_version_dependencies(
            self.ledger.pool(),
            artifacts
                .iter()
                .map(|summary| summary.current_version.clone())
                .collect(),
        )
        .await
        .map_err(|error| map_public_query_error(error, correlation_id))?;
        if current_versions.len() != artifacts.len() {
            return Err(runtime_error(
                ErrorCode::InternalInvariantViolated,
                correlation_id,
                false,
                "Artifact current version is missing",
            ));
        }
        for (summary, current_version) in artifacts.iter_mut().zip(current_versions) {
            summary.current_version = current_version;
        }
        let next_before_artifact_id = has_more
            .then(|| artifacts.last().map(|value| value.artifact.artifact_id))
            .flatten();
        artifacts.shrink_to_fit();
        Ok(ArtifactPage {
            artifacts,
            next_before_artifact_id,
            has_more,
        })
    }
}

impl ArtifactStore for PostgresArtifactStore {
    type Error = PostgresArtifactError;

    fn commit_tool_artifact(
        &self,
        command: CommitToolArtifact,
    ) -> musteragent_runtime_contract::ApplicationFuture<
        '_,
        Result<ArtifactCommitReceipt, Self::Error>,
    > {
        Box::pin(async move { self.commit_inner(command).await })
    }

    fn load<'a>(
        &'a self,
        tenant_id: TenantId,
        artifact_id: ArtifactId,
    ) -> musteragent_runtime_contract::ApplicationFuture<'a, Result<ArtifactView, Self::Error>>
    {
        Box::pin(async move { self.load_inner(tenant_id, artifact_id).await })
    }

    fn list(
        &self,
        query: ListArtifacts,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<Vec<Artifact>, Self::Error>>
    {
        Box::pin(async move { self.list_inner(query).await })
    }
}

impl ArtifactApplication for PostgresArtifactStore {
    fn get_artifact(
        &self,
        query: GetArtifact,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<ArtifactView, RuntimeError>>
    {
        Box::pin(async move { self.get_public_inner(query).await })
    }

    fn list_conversation_artifacts(
        &self,
        query: ListConversationArtifacts,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<ArtifactPage, RuntimeError>>
    {
        Box::pin(async move { self.list_public_inner(query).await })
    }
}

impl ArtifactContentAuthorization for PostgresArtifactStore {
    fn is_current_content_reachable(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        reference: &ContentRef,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<bool, RuntimeError>> {
        let reference = reference.clone();
        Box::pin(async move {
            let correlation_id = musteragent_identity::CorrelationId::generate();
            sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1
                     FROM artifact.artifacts AS artifact
                     JOIN artifact.artifact_versions AS version
                       ON version.artifact_id = artifact.artifact_id
                      AND version.version = artifact.current_version
                     WHERE artifact.tenant_id = $1
                       AND artifact.conversation_id = $2
                       AND (
                           EXISTS (
                               SELECT 1 FROM conversation.participants AS viewer
                               WHERE viewer.tenant_id = artifact.tenant_id
                                 AND viewer.conversation_id = artifact.conversation_id
                                 AND viewer.kind = 1
                                 AND viewer.principal_ref = $3
                                 AND viewer.left_at_ms IS NULL
                           )
                       )
                       AND (
                           EXISTS (
                               SELECT 1 FROM artifact.content_refs AS content
                               WHERE content.content_id = version.content_id
                                 AND content.tenant_id = artifact.tenant_id
                                 AND content.state = $8
                                 AND content.content_id = $4
                                 AND content.media_type = $5
                                 AND content.byte_length = $6::bigint
                                 AND content.content_hash = $7
                                 AND version.content_hash = content.content_hash
                           )
                           OR EXISTS (
                               SELECT 1
                               FROM artifact.artifact_version_dependencies AS dependency
                               JOIN artifact.content_refs AS content
                                 ON content.content_id = dependency.content_id
                                AND content.tenant_id = artifact.tenant_id
                                AND content.state = $8
                               WHERE dependency.artifact_version_id = version.artifact_version_id
                                 AND dependency.content_id = $4
                                 AND dependency.content_media_type = $5
                                 AND dependency.content_byte_length = $6::bigint
                                 AND dependency.content_hash = $7
                                 AND content.media_type = dependency.content_media_type
                                 AND content.byte_length = dependency.content_byte_length
                                 AND content.content_hash = dependency.content_hash
                           )
                       )
                 )",
            )
            .bind(actor.tenant_id.into_uuid())
            .bind(conversation_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .bind(reference.content_id.into_uuid())
            .bind(reference.media_type.as_str())
            .bind(reference.byte_length.to_string())
            .bind(reference.sha256.into_bytes().as_slice())
            .bind(CONTENT_READY)
            .fetch_one(self.ledger.pool())
            .await
            .map_err(|_| {
                runtime_error(
                    ErrorCode::DependencyUnavailable,
                    correlation_id,
                    true,
                    "Artifact authorization storage is unavailable",
                )
            })
        })
    }

    fn current_content_reachability<'a>(
        &'a self,
        actor: Actor,
        conversation_id: ConversationId,
        references: &'a [ContentRef],
    ) -> musteragent_runtime_contract::ApplicationFuture<'a, Result<Vec<bool>, RuntimeError>> {
        Box::pin(async move {
            if references.is_empty() {
                return Ok(Vec::new());
            }
            let correlation_id = CorrelationId::generate();
            let requested_ids = references
                .iter()
                .map(|reference| reference.content_id.into_uuid())
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                "WITH authorized_versions AS MATERIALIZED (
                     SELECT artifact.tenant_id, version.artifact_version_id,
                            version.content_id, version.content_hash
                     FROM artifact.artifacts AS artifact
                     JOIN artifact.artifact_versions AS version
                       ON version.artifact_id = artifact.artifact_id
                      AND version.version = artifact.current_version
                     WHERE artifact.tenant_id = $1
                       AND artifact.conversation_id = $2
                       AND (
                           EXISTS (
                               SELECT 1 FROM conversation.participants AS viewer
                               WHERE viewer.tenant_id = artifact.tenant_id
                                 AND viewer.conversation_id = artifact.conversation_id
                                 AND viewer.kind = 1
                                 AND viewer.principal_ref = $3
                                 AND viewer.left_at_ms IS NULL
                           )
                       )
                 ), reachable_content AS (
                     SELECT content.content_id, content.media_type,
                            content.byte_length, content.content_hash
                     FROM authorized_versions AS version
                     JOIN artifact.content_refs AS content
                       ON content.content_id = version.content_id
                      AND content.tenant_id = version.tenant_id
                      AND content.state = $5
                      AND content.content_hash = version.content_hash
                     WHERE content.content_id = ANY($4)
                     UNION
                     SELECT content.content_id, content.media_type,
                            content.byte_length, content.content_hash
                     FROM authorized_versions AS version
                     JOIN artifact.artifact_version_dependencies AS dependency
                       ON dependency.artifact_version_id = version.artifact_version_id
                     JOIN artifact.content_refs AS content
                       ON content.content_id = dependency.content_id
                      AND content.tenant_id = version.tenant_id
                      AND content.state = $5
                      AND content.media_type = dependency.content_media_type
                      AND content.byte_length = dependency.content_byte_length
                      AND content.content_hash = dependency.content_hash
                     WHERE content.content_id = ANY($4)
                 )
                 SELECT content_id, media_type, byte_length::text AS byte_length,
                        content_hash
                 FROM reachable_content",
            )
            .bind(actor.tenant_id.into_uuid())
            .bind(conversation_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .bind(requested_ids)
            .bind(CONTENT_READY)
            .fetch_all(self.ledger.pool())
            .await
            .map_err(|_| {
                runtime_error(
                    ErrorCode::DependencyUnavailable,
                    correlation_id,
                    true,
                    "Artifact authorization storage is unavailable",
                )
            })?;
            decode_content_reachability(rows, references, correlation_id)
        })
    }

    fn version_content_reachability<'a>(
        &'a self,
        actor: Actor,
        conversation_id: ConversationId,
        artifact_id: ArtifactId,
        artifact_version_id: ArtifactVersionId,
        references: &'a [ContentRef],
    ) -> musteragent_runtime_contract::ApplicationFuture<'a, Result<Vec<bool>, RuntimeError>> {
        self.version_reachability(
            actor,
            version_content::VersionReadScope::Conversation(conversation_id),
            artifact_id,
            artifact_version_id,
            references,
        )
    }

    fn workspace_version_content_reachability<'a>(
        &'a self,
        actor: Actor,
        workspace_id: musteragent_identity::WorkspaceId,
        artifact_id: ArtifactId,
        artifact_version_id: ArtifactVersionId,
        references: &'a [ContentRef],
    ) -> musteragent_runtime_contract::ApplicationFuture<'a, Result<Vec<bool>, RuntimeError>> {
        self.version_reachability(
            actor,
            version_content::VersionReadScope::Workspace(workspace_id),
            artifact_id,
            artifact_version_id,
            references,
        )
    }

    fn resolve_current_content(
        &self,
        actor: Actor,
        conversation_id: ConversationId,
        content_id: musteragent_identity::ContentId,
    ) -> musteragent_runtime_contract::ApplicationFuture<'_, Result<Option<ContentRef>, RuntimeError>>
    {
        Box::pin(async move {
            let correlation_id = CorrelationId::generate();
            let row = sqlx::query(
                "SELECT content.content_id, content.media_type,
                        content.byte_length::text AS byte_length, content.content_hash
                 FROM artifact.content_refs AS content
                 WHERE content.tenant_id = $1
                   AND content.content_id = $4
                   AND content.state = $5
                   AND EXISTS (
                       SELECT 1
                       FROM artifact.artifacts AS artifact
                       JOIN artifact.artifact_versions AS version
                         ON version.artifact_id = artifact.artifact_id
                        AND version.version = artifact.current_version
                       WHERE artifact.tenant_id = $1
                         AND artifact.conversation_id = $2
                         AND EXISTS (
                             SELECT 1 FROM conversation.participants AS viewer
                             WHERE viewer.tenant_id = artifact.tenant_id
                               AND viewer.conversation_id = artifact.conversation_id
                               AND viewer.kind = 1
                               AND viewer.principal_ref = $3
                               AND viewer.left_at_ms IS NULL
                         )
                         AND (
                             (version.content_id = content.content_id
                              AND version.content_hash = content.content_hash)
                             OR EXISTS (
                                 SELECT 1
                                 FROM artifact.artifact_version_dependencies AS dependency
                                 WHERE dependency.artifact_version_id = version.artifact_version_id
                                   AND dependency.content_id = content.content_id
                                   AND dependency.content_media_type = content.media_type
                                   AND dependency.content_byte_length = content.byte_length
                                   AND dependency.content_hash = content.content_hash
                             )
                         )
                   )
                 LIMIT 1",
            )
            .bind(actor.tenant_id.into_uuid())
            .bind(conversation_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .bind(content_id.into_uuid())
            .bind(CONTENT_READY)
            .fetch_optional(self.ledger.pool())
            .await
            .map_err(|_| {
                runtime_error(
                    ErrorCode::DependencyUnavailable,
                    correlation_id,
                    true,
                    "Artifact authorization storage is unavailable",
                )
            })?;
            row.map(|row| -> Result<ContentRef, sqlx::Error> {
                Ok(ContentRef {
                    content_id: musteragent_identity::ContentId::from_uuid(
                        row.try_get("content_id")?,
                    )
                    .map_err(|_| sqlx::Error::ColumnDecode {
                        index: "content_id".into(),
                        source: Box::new(PostgresArtifactError::Corrupt),
                    })?,
                    media_type: MediaType::new(row.try_get::<String, _>("media_type")?).map_err(
                        |_| sqlx::Error::ColumnDecode {
                            index: "media_type".into(),
                            source: Box::new(PostgresArtifactError::Corrupt),
                        },
                    )?,
                    byte_length: parse_u64(row.try_get::<String, _>("byte_length")?).map_err(
                        |_| sqlx::Error::ColumnDecode {
                            index: "byte_length".into(),
                            source: Box::new(PostgresArtifactError::Corrupt),
                        },
                    )?,
                    sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                        .map_err(|_| sqlx::Error::ColumnDecode {
                            index: "content_hash".into(),
                            source: Box::new(PostgresArtifactError::Corrupt),
                        })?,
                })
            })
            .transpose()
            .map_err(|_| {
                runtime_error(
                    ErrorCode::InternalInvariantViolated,
                    correlation_id,
                    false,
                    "Artifact authorization state is corrupt",
                )
            })
        })
    }
}

struct InvocationOwnership {
    conversation_id: ConversationId,
    run_id: RunId,
}

async fn lock_invocation(
    transaction: &mut sqlx::PgConnection,
    command: &CommitToolArtifact,
) -> Result<InvocationOwnership, PostgresArtifactError> {
    let row = sqlx::query(
        "SELECT conversation_id, run_id, status
         FROM tool.tool_invocations
         WHERE tenant_id = $1 AND invocation_id = $2
         FOR UPDATE",
    )
    .bind(command.tenant_id.into_uuid())
    .bind(command.invocation_id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(PostgresArtifactError::InvocationNotFound)?;
    if row.try_get::<i16, _>("status")? != TOOL_RUNNING {
        return Err(PostgresArtifactError::ToolTransitionConflict);
    }
    let conversation_id = ConversationId::from_uuid(row.try_get("conversation_id")?)
        .map_err(|_| PostgresArtifactError::Corrupt)?;
    if conversation_id != command.append.conversation_id {
        return Err(PostgresArtifactError::IdentityMismatch);
    }
    Ok(InvocationOwnership {
        conversation_id,
        run_id: RunId::from_uuid(row.try_get("run_id")?)
            .map_err(|_| PostgresArtifactError::Corrupt)?,
    })
}

pub(crate) async fn authorize_content(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    version: &ArtifactVersion,
) -> Result<(), PostgresArtifactError> {
    let authorized: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.content_refs
             WHERE tenant_id = $1 AND content_id = $2 AND state = $3
               AND content_hash = $4 AND byte_length = $5::bigint
               AND media_type = $6
         )",
    )
    .bind(tenant_id.into_uuid())
    .bind(version.content_ref.content_id.into_uuid())
    .bind(CONTENT_READY)
    .bind(version.content_hash.into_bytes().as_slice())
    .bind(version.content_ref.byte_length.to_string())
    .bind(version.content_ref.media_type.as_str())
    .fetch_one(&mut *transaction)
    .await?;
    if !authorized {
        return Err(PostgresArtifactError::ContentUnavailable);
    }
    for dependency in &version.dependency_refs {
        authorize_content_ref(transaction, tenant_id, dependency).await?;
    }
    Ok(())
}

pub(crate) async fn authorize_content_ref(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    reference: &ContentRef,
) -> Result<(), PostgresArtifactError> {
    let authorized = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.content_refs
             WHERE tenant_id = $1 AND content_id = $2 AND state = $3
               AND content_hash = $4 AND byte_length = $5::bigint
               AND media_type = $6
         )",
    )
    .bind(tenant_id.into_uuid())
    .bind(reference.content_id.into_uuid())
    .bind(CONTENT_READY)
    .bind(reference.sha256.into_bytes().as_slice())
    .bind(reference.byte_length.to_string())
    .bind(reference.media_type.as_str())
    .fetch_one(&mut *transaction)
    .await?;
    if authorized {
        Ok(())
    } else {
        Err(PostgresArtifactError::ContentUnavailable)
    }
}

pub(crate) async fn authorize_result_content(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    result: &musteragent_runtime_capability::CapabilityResult,
) -> Result<(), PostgresArtifactError> {
    let authorized: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM artifact.content_refs
             WHERE tenant_id = $1 AND content_id = $2 AND state = $3
               AND content_hash = $4 AND byte_length = $5::bigint
               AND media_type = $6
         )",
    )
    .bind(tenant_id.into_uuid())
    .bind(result.result_ref.content_id.into_uuid())
    .bind(CONTENT_READY)
    .bind(result.result_hash.into_bytes().as_slice())
    .bind(result.result_ref.byte_length.to_string())
    .bind(result.result_ref.media_type.as_str())
    .fetch_one(&mut *transaction)
    .await?;
    if authorized {
        Ok(())
    } else {
        Err(PostgresArtifactError::ContentUnavailable)
    }
}

pub(crate) async fn insert_artifact(
    transaction: &mut sqlx::PgConnection,
    artifact: &Artifact,
) -> Result<(), PostgresArtifactError> {
    let (origin, activity_id, created_by_run_id) = match artifact.origin {
        ArtifactOrigin::Runtime {
            activity_id,
            created_by_run_id,
        } => (
            1_i16,
            Some(activity_id.into_uuid()),
            Some(created_by_run_id.into_uuid()),
        ),
        ArtifactOrigin::GovernedImport => (2_i16, None, None),
        ArtifactOrigin::Product => (3_i16, None, None),
    };
    sqlx::query(
        "INSERT INTO artifact.artifacts (
             artifact_id, tenant_id, conversation_id, activity_id,
             created_by_run_id, kind, lifecycle, current_version,
             title, created_at_ms, updated_at_ms, origin, workspace_id
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8::bigint, $9, $10, $11, $12, $13
         )",
    )
    .bind(artifact.artifact_id.into_uuid())
    .bind(artifact.tenant_id.into_uuid())
    .bind(artifact.source.conversation_id().map(|id| id.into_uuid()))
    .bind(activity_id)
    .bind(created_by_run_id)
    .bind(kind_code(artifact.kind))
    .bind(lifecycle_code(artifact.lifecycle))
    .bind(artifact.current_version.get().to_string())
    .bind(&artifact.title)
    .bind(artifact.created_at.unix_millis())
    .bind(artifact.updated_at.unix_millis())
    .bind(origin)
    .bind(artifact.source.workspace_id().map(|id| id.into_uuid()))
    .execute(&mut *transaction)
    .await
    .map_err(map_artifact_insert)?;
    Ok(())
}

pub(crate) async fn update_artifact(
    transaction: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    conversation_id: ConversationId,
    artifact_id: ArtifactId,
    expected_version: VersionNumber,
    version: &ArtifactVersion,
    title: Option<&str>,
) -> Result<(), PostgresArtifactError> {
    let updated = sqlx::query(
        "UPDATE artifact.artifacts
         SET current_version = $5::bigint,
             title = COALESCE($6, title),
             title_source_invocation_id = CASE WHEN $6 IS NULL THEN title_source_invocation_id ELSE $8 END,
             title_source_known = CASE WHEN $6 IS NULL THEN title_source_known ELSE true END,
             title_erasure_job_id = CASE WHEN $6 IS NULL THEN title_erasure_job_id ELSE NULL END,
             updated_at_ms = $7
         WHERE tenant_id = $1 AND conversation_id = $2 AND artifact_id = $3
           AND current_version = $4::bigint AND lifecycle IN (1, 2)",
    )
    .bind(tenant_id.into_uuid())
    .bind(conversation_id.into_uuid())
    .bind(artifact_id.into_uuid())
    .bind(expected_version.get().to_string())
    .bind(version.version.get().to_string())
    .bind(title)
    .bind(version.committed_at.unix_millis())
    .bind(version.source_invocation_id.map(InvocationId::into_uuid))
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PostgresArtifactError::VersionConflict)
    }
}

pub(crate) async fn insert_version(
    transaction: &mut sqlx::PgConnection,
    version: &ArtifactVersion,
) -> Result<(), PostgresArtifactError> {
    sqlx::query(
        "INSERT INTO artifact.artifact_versions (
             artifact_version_id, artifact_id, version, content_id,
             content_hash, source_invocation_id, committed_by, committed_at_ms, committed_by_principal_id
         ) VALUES ($1, $2, $3::bigint, $4, $5, $6, $7, $8, $9)",
    )
    .bind(version.artifact_version_id.into_uuid())
    .bind(version.artifact_id.into_uuid())
    .bind(version.version.get().to_string())
    .bind(version.content_ref.content_id.into_uuid())
    .bind(version.content_hash.into_bytes().as_slice())
    .bind(version.source_invocation_id.map(InvocationId::into_uuid))
    .bind(version.committed_by.participant_id().map(|id| id.into_uuid()))
    .bind(version.committed_at.unix_millis())
    .bind(version.committed_by.principal_id().map(|id| id.into_uuid()))
    .execute(&mut *transaction)
    .await
    .map_err(map_version_insert)?;
    if version.version.get() == 1 {
        sqlx::query("UPDATE artifact.artifacts SET title_source_invocation_id=$2,title_source_known=true WHERE artifact_id=$1 AND current_version=1")
            .bind(version.artifact_id.into_uuid())
            .bind(version.source_invocation_id.map(InvocationId::into_uuid))
            .execute(&mut *transaction).await?;
    }
    for (ordinal, dependency) in version.dependency_refs.iter().enumerate() {
        sqlx::query(
            "INSERT INTO artifact.artifact_version_dependencies (
                 artifact_version_id, dependency_ordinal, content_id,
                 content_media_type, content_byte_length, content_hash
             ) VALUES ($1, $2, $3, $4, $5::bigint, $6)",
        )
        .bind(version.artifact_version_id.into_uuid())
        .bind(i16::try_from(ordinal).map_err(|_| PostgresArtifactError::Corrupt)?)
        .bind(dependency.content_id.into_uuid())
        .bind(dependency.media_type.as_str())
        .bind(dependency.byte_length.to_string())
        .bind(dependency.sha256.into_bytes().as_slice())
        .execute(&mut *transaction)
        .await?;
    }
    Ok(())
}

pub(crate) async fn complete_tool(
    transaction: &mut sqlx::PgConnection,
    keyring: &OpaqueRefKeyring,
    tenant_id: TenantId,
    invocation_id: InvocationId,
    result: &musteragent_runtime_capability::CapabilityResult,
    finished_at: UtcTimestamp,
    allow_reconciled_indeterminate: bool,
) -> Result<(), PostgresArtifactError> {
    let provider_request_ref = result
        .provider_request_ref
        .as_ref()
        .map(|value| {
            let context = format!("tool-invocation:{invocation_id}:provider-request");
            keyring
                .seal(context.as_bytes(), value.as_str())
                .map(|protected| protected.as_bytes().to_vec())
        })
        .transpose()?;
    let updated = sqlx::query(
        "UPDATE tool.tool_invocations
         SET status = $3, finished_at_ms = $4, result_content_id = $5,
             terminal_error = NULL, terminal_reason = NULL,
             reconciliation_ref_protected = NULL
         WHERE tenant_id = $1 AND invocation_id = $2
           AND (
               status = $6
               OR ($7 AND status = 6 AND retry_contract = 3)
           )",
    )
    .bind(tenant_id.into_uuid())
    .bind(invocation_id.into_uuid())
    .bind(TOOL_SUCCEEDED)
    .bind(finished_at.unix_millis())
    .bind(result.result_ref.content_id.into_uuid())
    .bind(TOOL_RUNNING)
    .bind(allow_reconciled_indeterminate)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(PostgresArtifactError::ToolTransitionConflict);
    }
    sqlx::query(
        "INSERT INTO tool.tool_results (
             invocation_id, result_content_id, result_hash,
             result_schema_key, result_schema_hash,
             provider_request_ref_protected, committed_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(invocation_id.into_uuid())
    .bind(result.result_ref.content_id.into_uuid())
    .bind(result.result_hash.into_bytes().as_slice())
    .bind(result.result_schema.schema_key.as_str())
    .bind(result.result_schema.schema_hash.into_bytes().as_slice())
    .bind(provider_request_ref)
    .bind(finished_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

async fn insert_commit_command(
    transaction: &mut sqlx::PgConnection,
    command: &CommitToolArtifact,
    sequences: &[musteragent_identity::ConversationSeq],
) -> Result<(), PostgresArtifactError> {
    let first = sequences
        .first()
        .ok_or(PostgresArtifactError::InvalidLedger)?
        .get();
    let last = sequences
        .last()
        .ok_or(PostgresArtifactError::InvalidLedger)?
        .get();
    sqlx::query(
        "INSERT INTO artifact.commit_commands (
             tenant_id, idempotency_key, command_fingerprint, invocation_id,
             artifact_id, artifact_version_id, ledger_first_sequence,
             ledger_last_sequence, committed_at_ms
         ) VALUES ($1, $2, $3, $4, $5, $6, $7::bigint, $8::bigint, $9)",
    )
    .bind(command.tenant_id.into_uuid())
    .bind(command.append.idempotency_key.as_str())
    .bind(command.append.command_fingerprint.into_bytes().as_slice())
    .bind(command.invocation_id.into_uuid())
    .bind(command.mutation.artifact_id().into_uuid())
    .bind(command.mutation.version().artifact_version_id.into_uuid())
    .bind(first.to_string())
    .bind(last.to_string())
    .bind(command.mutation.version().committed_at.unix_millis())
    .execute(&mut *transaction)
    .await?;
    Ok(())
}

pub(crate) async fn load_version_dependencies<
    'e,
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
>(
    executor: E,
    mut versions: Vec<ArtifactVersion>,
) -> Result<Vec<ArtifactVersion>, PostgresArtifactError> {
    if versions.is_empty() {
        return Ok(versions);
    }
    let version_ids = versions
        .iter()
        .map(|version| version.artifact_version_id.into_uuid())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        "SELECT dependency.artifact_version_id, dependency.dependency_ordinal,
                dependency.content_id, dependency.content_media_type,
                dependency.content_byte_length::text AS content_byte_length,
                dependency.content_hash, content.state,
                content.tenant_id AS content_tenant_id,
                artifact.tenant_id AS artifact_tenant_id,
                content.media_type AS actual_media_type,
                content.byte_length::text AS actual_byte_length,
                content.content_hash AS actual_hash
         FROM artifact.artifact_version_dependencies AS dependency
         JOIN artifact.artifact_versions AS version
           ON version.artifact_version_id = dependency.artifact_version_id
         JOIN artifact.artifacts AS artifact
           ON artifact.artifact_id = version.artifact_id
         JOIN artifact.content_refs AS content
           ON content.content_id = dependency.content_id
         WHERE dependency.artifact_version_id = ANY($1)
         ORDER BY dependency.artifact_version_id, dependency.dependency_ordinal",
    )
    .bind(&version_ids)
    .fetch_all(executor)
    .await?;
    let mut dependencies = BTreeMap::<ArtifactVersionId, Vec<ContentRef>>::new();
    for row in rows {
        let artifact_version_id = id(row.try_get("artifact_version_id")?)?;
        let ordinal = usize::try_from(row.try_get::<i16, _>("dependency_ordinal")?)
            .map_err(|_| PostgresArtifactError::Corrupt)?;
        let target = dependencies.entry(artifact_version_id).or_default();
        if ordinal != target.len()
            || row.try_get::<i16, _>("state")? != CONTENT_READY
            || row.try_get::<uuid::Uuid, _>("content_tenant_id")?
                != row.try_get::<uuid::Uuid, _>("artifact_tenant_id")?
            || row.try_get::<String, _>("content_media_type")?
                != row.try_get::<String, _>("actual_media_type")?
            || row.try_get::<String, _>("content_byte_length")?
                != row.try_get::<String, _>("actual_byte_length")?
            || row.try_get::<Vec<u8>, _>("content_hash")?
                != row.try_get::<Vec<u8>, _>("actual_hash")?
        {
            return Err(PostgresArtifactError::Corrupt);
        }
        target.push(ContentRef {
            content_id: id(row.try_get("content_id")?)?,
            media_type: MediaType::new(row.try_get::<String, _>("content_media_type")?)
                .map_err(|_| PostgresArtifactError::Corrupt)?,
            byte_length: parse_u64(row.try_get("content_byte_length")?)?,
            sha256: Sha256::from_bytes(row.try_get::<Vec<u8>, _>("content_hash")?)
                .map_err(|_| PostgresArtifactError::Corrupt)?,
        });
    }
    for version in &mut versions {
        version.dependency_refs = dependencies
            .remove(&version.artifact_version_id)
            .unwrap_or_default();
        version
            .validate()
            .map_err(|_| PostgresArtifactError::Corrupt)?;
    }
    if !dependencies.is_empty() {
        return Err(PostgresArtifactError::Corrupt);
    }
    Ok(versions)
}

#[derive(Debug, Error)]
pub enum PostgresArtifactError {
    #[error(transparent)]
    Contract(#[from] ArtifactRuntimeError),
    #[error(transparent)]
    Ledger(#[from] PostgresLedgerError),
    #[error(transparent)]
    Protection(#[from] OpaqueRefProtectionError),
    #[error("Artifact commit identity conflicts with the durable Tool invocation")]
    IdentityMismatch,
    #[error("Tool invocation was not found")]
    InvocationNotFound,
    #[error("Tool invocation is not in the required Running state")]
    ToolTransitionConflict,
    #[error("Artifact content is unavailable or not authorized for the tenant")]
    ContentUnavailable,
    #[error("Artifact already exists")]
    ArtifactConflict,
    #[error("Artifact version has advanced or conflicts")]
    VersionConflict,
    #[error("Artifact was not found")]
    NotFound,
    #[error("Artifact commit idempotency identity conflicts")]
    IdempotencyConflict,
    #[error("Artifact commit contains no Ledger sequence")]
    InvalidLedger,
    #[error("persisted Artifact data is corrupt")]
    Corrupt,
    #[error(transparent)]
    Transaction(#[from] crate::PostgresTransactionError),
    #[error("PostgreSQL Artifact operation failed")]
    Sql(#[from] sqlx::Error),
}

impl ClassifyArtifactReadError for PostgresArtifactError {
    fn artifact_read_failure(&self) -> ArtifactReadFailure {
        match self {
            Self::NotFound => ArtifactReadFailure::NotFound,
            Self::Sql(_) | Self::Transaction(_) => ArtifactReadFailure::Unavailable,
            Self::Corrupt
            | Self::Contract(_)
            | Self::Ledger(_)
            | Self::Protection(_)
            | Self::IdentityMismatch
            | Self::InvocationNotFound
            | Self::ToolTransitionConflict
            | Self::ContentUnavailable
            | Self::ArtifactConflict
            | Self::VersionConflict
            | Self::IdempotencyConflict
            | Self::InvalidLedger => ArtifactReadFailure::Corrupt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_diagram_kind_keeps_its_historical_storage_contract() {
        assert_eq!(kind_code(ArtifactKind::LegacyDiagram), 2);
        assert!(matches!(decode_kind(2), Ok(ArtifactKind::LegacyDiagram)));
    }
}
