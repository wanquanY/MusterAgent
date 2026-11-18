//! Dialogue Publication projects the original Device Interaction and transports
//! commands. Only the signed Device Owner acknowledgement settles a command.
use super::device_tool::source;
use super::*;
use musteragent_approval::{InteractionKind, InteractionStatus};
use musteragent_content_ref::{ContentRef, Sha256};
use musteragent_persistence_codec::native_dialogue_interaction_codec as native_codec;
use musteragent_runtime_content::{ContentStore, PutContent};
use musteragent_runtime_contract::{CommandAck, CommandMetadata, ErrorCode, RuntimeError};
use prost::Message;
use std::sync::Arc;
mod context;
mod controls;
mod eligibility;
pub(super) mod publication;

#[derive(Clone)]
pub struct PostgresNativeDialogueInteractionStore {
    database: crate::PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
    targets: Arc<dyn WorkItemDeviceTargetAuthority>,
    controls: Arc<dyn NativeDialogueInteractionControlAuthorizer>,
}
impl PostgresNativeDialogueInteractionStore {
    /// Search uses the same retained publication and integrity checks as the
    /// public card. Reading its prompt neither requires a live Device nor
    /// exposes answers, control commands, or private execution context.
    pub(crate) async fn published_prompt(
        conn: &mut sqlx::PgConnection,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
    ) -> Result<Option<(InteractionKind, ContentRef)>, RuntimeError> {
        let Some(row) = controls::entry_row(conn, actor, dialogue, entry, false).await? else {
            return Ok(None);
        };
        let projection = row_projection(&row)?;
        Ok(Some((
            projection.interaction.kind,
            projection.interaction.prompt_ref,
        )))
    }

    /// Memory consumes the authenticated original Native observation; it does
    /// not create or settle a Cloud Interaction on behalf of a Device.
    pub(crate) async fn approved_memory_interaction(
        conn: &mut sqlx::PgConnection,
        interaction: InteractionId,
    ) -> Result<Option<(Actor, AgentId, NativeDialogueInteractionProjection)>, RuntimeError> {
        let row = sqlx::query("SELECT * FROM agent.native_dialogue_interaction_projections WHERE interaction_id=$1 AND status=2 AND NOT control_open")
            .bind(interaction.into_uuid()).fetch_optional(conn).await.map_err(database)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let projection = row_projection(&row)?;
        projection.validate().map_err(fail)?;
        if projection.interaction.kind != InteractionKind::Approval
            || projection.resolution != Some(DialogueInteractionResolution::Approved)
        {
            return Ok(None);
        }
        Ok(Some((
            Actor {
                tenant_id: row_id(&row, "tenant_id")?,
                principal_id: row_id(&row, "owner_id")?,
                principal_kind: PrincipalKind::User,
            },
            row_id(&row, "agent_id")?,
            projection,
        )))
    }

    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        targets: Arc<dyn WorkItemDeviceTargetAuthority>,
        controls: Arc<dyn NativeDialogueInteractionControlAuthorizer>,
    ) -> Self {
        Self {
            database: database.into(),
            content,
            targets,
            controls,
        }
    }
    async fn stage(&self, actor: Actor, r: &ContentRef, bytes: &[u8]) -> Result<(), RuntimeError> {
        let stored = self
            .content
            .put_at(
                r.content_id,
                PutContent {
                    tenant_id: actor.tenant_id,
                    media_type: r.media_type.clone(),
                    bytes: bytes.to_vec(),
                },
            )
            .await
            .map_err(|_| fail(ErrorCode::DependencyUnavailable))?;
        if stored != *r {
            return Err(fail(ErrorCode::ContentIntegrityFailed));
        }
        Ok(())
    }
    async fn stage_projection(
        &self,
        actor: Actor,
        p: &NativeDialogueInteractionProjection,
    ) -> Result<(), RuntimeError> {
        self.stage(actor, &p.interaction.prompt_ref, &p.prompt_bytes)
            .await?;
        if let Some(DialogueInteractionResolution::Answer { reference, bytes }) = &p.resolution {
            self.stage(actor, reference, bytes).await?;
        }
        Ok(())
    }
}
impl NativeDialogueInteractionStore for PostgresNativeDialogueInteractionStore {
    fn read_control(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        command: CommandId,
    ) -> ApplicationFuture<'_, Result<DialogueInteractionControlReceipt, RuntimeError>> {
        Box::pin(async move {
            self.read_control_inner(actor, dialogue, entry, command)
                .await
        })
    }
    fn publish<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        projection: &'a NativeDialogueInteractionProjection,
    ) -> ApplicationFuture<'a, Result<NativeDialogueInteractionPublicationReceipt, RuntimeError>>
    {
        Box::pin(self.publish_inner(actor, target, projection))
    }
    fn commands<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        namespace: NativeDialogueInteractionInstallationNamespace,
        after: Option<NativeDialogueInteractionCommandCursor>,
        limit: u32,
    ) -> ApplicationFuture<'a, Result<NativeDialogueInteractionCommandPage, RuntimeError>> {
        Box::pin(self.commands_inner(actor, target, namespace, after, limit))
    }
    fn acknowledge<'a>(
        &'a self,
        actor: Actor,
        target: &'a OriginDeviceExecutionTarget,
        command: &'a NativeDialogueInteractionCommand,
        projection: &'a NativeDialogueInteractionProjection,
        outcome: Result<CommandAck, ErrorCode>,
    ) -> ApplicationFuture<'a, Result<NativeDialogueInteractionAcknowledgement, RuntimeError>> {
        Box::pin(self.acknowledge_inner(actor, target, command, projection, outcome))
    }
    fn read(
        &self,
        actor: Actor,
        dialogue: AgentDialogueId,
        entry: DialogueEntryId,
        correlation: CorrelationId,
    ) -> ApplicationFuture<'_, Result<Option<DialogueInteractionView>, RuntimeError>> {
        Box::pin(async move {
            self.read_inner(actor, dialogue, entry, correlation)
                .await
                .map_err(|mut e| {
                    e.correlation_id = correlation;
                    e
                })
        })
    }
    fn submit(
        &self,
        metadata: CommandMetadata,
        target: DialogueInteractionTarget,
        response: DialogueInteractionControlResponse,
    ) -> ApplicationFuture<'_, Result<Option<DialogueInteractionControlReceipt>, RuntimeError>>
    {
        Box::pin(self.submit_inner(metadata, target, response))
    }
}
fn fail(code: ErrorCode) -> RuntimeError {
    runtime_error(
        code,
        CorrelationId::generate(),
        code == ErrorCode::DependencyUnavailable,
        "Native Dialogue interaction authority or observation is unavailable",
    )
}
fn database(_: impl std::fmt::Debug) -> RuntimeError {
    fail(ErrorCode::DependencyUnavailable)
}
fn corrupt(_: impl std::fmt::Debug) -> RuntimeError {
    fail(ErrorCode::ContentIntegrityFailed)
}
fn row_projection(row: &PgRow) -> Result<NativeDialogueInteractionProjection, RuntimeError> {
    let p = native_codec::decode_projection_bytes(
        &row.try_get::<Vec<u8>, _>("projection_bytes")
            .map_err(database)?,
    )
    .map_err(corrupt)?;
    for (name, h) in [
        ("request_sha256", p.request_fingerprint()),
        ("state_sha256", p.state_fingerprint()),
        ("projection_sha256", p.fingerprint()),
    ] {
        if row.try_get::<Vec<u8>, _>(name).map_err(database)? != h.into_bytes() {
            return Err(fail(ErrorCode::ContentIntegrityFailed));
        }
    }
    Ok(p)
}
fn row_id<T: std::str::FromStr>(row: &PgRow, name: &str) -> Result<T, RuntimeError> {
    row.try_get::<uuid::Uuid, _>(name)
        .map_err(database)?
        .to_string()
        .parse()
        .map_err(|_| fail(ErrorCode::ContentIntegrityFailed))
}
fn status(p: &NativeDialogueInteractionProjection) -> i16 {
    match p.interaction.status {
        InteractionStatus::Pending => 1,
        InteractionStatus::Responded => 2,
        InteractionStatus::Expired => 3,
        InteractionStatus::Cancelled => 4,
    }
}
async fn receipt(
    conn: &mut sqlx::PgConnection,
    p: &NativeDialogueInteractionProjection,
) -> Result<NativeDialogueInteractionPublicationReceipt, RuntimeError> {
    let r = sqlx::query("SELECT dialogue_id,entry_id FROM agent.dialogue_publications WHERE source_interaction_id=$1 AND source_run_id=$2 AND execution_home='device'")
        .bind(p.interaction.interaction_id.into_uuid()).bind(p.admission.run_id.into_uuid()).fetch_one(conn).await.map_err(database)?;
    Ok(NativeDialogueInteractionPublicationReceipt {
        interaction_id: p.interaction.interaction_id,
        dialogue_id: row_id(&r, "dialogue_id")?,
        entry_id: row_id(&r, "entry_id")?,
        projection_sha256: p.fingerprint(),
        control_revision: p.state_fingerprint(),
    })
}

fn source_error(error: DialogueToolPublicationError) -> RuntimeError {
    fail(match error {
        DialogueToolPublicationError::Infrastructure => ErrorCode::DependencyUnavailable,
        DialogueToolPublicationError::Corrupt => ErrorCode::ContentIntegrityFailed,
        _ => ErrorCode::PolicyDenied,
    })
}
