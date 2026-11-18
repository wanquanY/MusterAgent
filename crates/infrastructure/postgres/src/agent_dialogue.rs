//! Input acceptance and public history share a durable per-dialogue sequence.
pub(crate) mod attachments;
pub(crate) mod capability;
mod execution_target;
pub(crate) mod input_contract;
mod progress;
mod reactions;
mod receipt;
pub(crate) mod reply_retention;
mod search;
mod task_target;
use crate::dialogue_codec::*;
use musteragent_execution_context::*;
use musteragent_identity::*;
use musteragent_persistence_codec::wire;
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use sqlx::{PgPool, Row, postgres::PgRow};
type Error = DialogueStoreError;
#[derive(Clone)]
pub struct PostgresAgentDialogueStore {
    pool: PgPool,
    pub(crate) database: crate::PostgresDatabaseRuntime,
}
impl PostgresAgentDialogueStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        Self {
            pool: database.pool().clone(),
            database,
        }
    }
    async fn accept_inner(
        &self,
        command: AcceptDialogueIntent,
        target: DialogueIntentExecutionTarget,
    ) -> Result<AcceptedDialogueIntent, Error> {
        if matches!(
            command.origin.surface,
            DialogueInputSurface::Schedule | DialogueInputSurface::Capability
        ) {
            // Internal continuations must prove their original admission authority.
            return Err(Error::Unavailable);
        }
        let mut tx = self
            .database
            .write_unit_of_work(musteragent_telemetry::DbCommitKind::Admission)
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let accepted = accept_on_target_in_transaction(&mut tx, command, target).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(accepted)
    }
}
/// Shared canonical writer; only trusted in-crate owners may add provenance
/// in the same transaction. This never starts an independent execution Run.
pub(crate) async fn accept_in_transaction(
    tx: &mut sqlx::PgConnection,
    command: AcceptDialogueIntent,
) -> Result<AcceptedDialogueIntent, Error> {
    accept_on_target_in_transaction(tx, command, DialogueIntentExecutionTarget::Cloud).await
}
async fn accept_on_target_in_transaction(
    tx: &mut sqlx::PgConnection,
    command: AcceptDialogueIntent,
    target: DialogueIntentExecutionTarget,
) -> Result<AcceptedDialogueIntent, Error> {
    command.validate().map_err(|_| Error::Unavailable)?;
    target.validate_for(&command)?;
    if command.task_target.is_some()
        && matches!(target, DialogueIntentExecutionTarget::OriginDevice(_))
    {
        return Err(Error::Unavailable);
    }
    let actor = command.actor;
    // A replay is still authorized by the current Native membership. Only its
    // immutable original destination/proof determines the retained receipt hash.
    if let DialogueIntentExecutionTarget::OriginDevice(device) = &target {
        device.require_current_at(command.accepted_at)?;
        execution_target::require_release()?;
    }
    let hash = execution_target::acceptance_fingerprint(&mut *tx, &command, &target).await?;
    if let Some(receipt) =
        command_receipt(tx, actor, command.command_id, "accept_intent", hash).await?
    {
        let Some(wire::owner_command_receipt::Result::AcceptedIntent(value)) = receipt.result
        else {
            return Err(Error::Corrupt);
        };
        let result = AcceptedDialogueIntent {
            intent: decode_intent(required(value.intent)?)?,
            replayed: true,
        };
        if !result.intent.input_attachments.is_empty() {
            attachments::verify(tx, &result.intent).await?;
        }
        return Ok(result);
    }
    // Only a verified desktop turn may address a scene outside the account default.
    let desktop_scene = command.origin.surface == DialogueInputSurface::Desktop
        && command.origin.installation_id.is_some()
        && matches!(
            target,
            DialogueIntentExecutionTarget::DesktopTools(_)
                | DialogueIntentExecutionTarget::CloudFromDesktop(_)
                | DialogueIntentExecutionTarget::OriginDevice(_)
        );
    // Fixed lock order across all dialogue writes: command -> space -> dialogue.
    let owned_space = sqlx::query(
        "SELECT s.space_id
                 FROM agent.owner_spaces s
                 JOIN agent.owned_agents a USING(space_id)
                 WHERE a.dialogue_id=$1
                 AND s.tenant_id=$2
                 AND s.owner_id=$3
                 AND EXISTS (
                     SELECT 1 FROM agent.owned_agent_scenes scene
                     WHERE scene.agent_id = a.agent_id AND scene.space_id = s.space_id
                       AND (scene.primary_agent_id = s.default_agent_id OR $4)
                 )
                 FOR UPDATE OF s",
    )
    .bind(command.dialogue_id.into_uuid())
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .bind(desktop_scene)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?;
    if owned_space.is_none() {
        return Err(Error::Unavailable);
    }
    let row = sqlx::query(
        "SELECT d.*,a.status
                 FROM agent.agent_dialogues d
                 JOIN agent.owned_agents a USING(agent_id)
                 WHERE d.dialogue_id=$1
                 AND d.tenant_id=$2
                 AND d.owner_id=$3
                 FOR UPDATE OF d
                 FOR SHARE OF a",
    )
    .bind(command.dialogue_id.into_uuid())
    .bind(actor.tenant_id.into_uuid())
    .bind(actor.principal_id.into_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(db)?
    .ok_or(Error::Unavailable)?;
    if row.try_get::<&str, _>("status").map_err(db)? != "active" {
        return Err(Error::Inactive);
    }
    let dialogue = dialogue_row(&row)?;
    command
        .observed_cursor
        .require_at_or_before(dialogue.head)
        .map_err(|_| Error::RevisionConflict)?;
    if let Some(reply) = command.reply_to_intent_id {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1
                 FROM agent.dialogue_user_intents
                 WHERE intent_id=$1
                 AND dialogue_id=$2
                 AND tenant_id=$3
                 AND owner_id=$4)",
        )
        .bind(reply.into_uuid())
        .bind(dialogue.dialogue_id.into_uuid())
        .bind(actor.tenant_id.into_uuid())
        .bind(actor.principal_id.into_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if !exists {
            return Err(Error::Unavailable);
        }
    }
    // Readiness is checked inside the original acceptance transaction, after
    // immutable receipt recovery and before new entries, retention or outbox.
    input_contract::require(tx, actor, dialogue.agent_id).await?;
    attachments::authorize(tx, &command).await?;
    task_target::authorize(tx, actor, &dialogue, command.task_target).await?;
    let reply_binding = match command.reply_target {
        Some(target) => Some(
            crate::dialogue_publication::reply::resolve(
                tx,
                actor,
                &dialogue,
                command.observed_cursor,
                target,
            )
            .await?,
        ),
        None => None,
    };
    // Explicit task selection must match the action frozen at acceptance.
    // The selected historical publication remains separate read-only context.
    if let (Some(task), Some(reply)) = (command.task_target, reply_binding.as_ref())
        && reply.task_action().map(|action| action.target) != Some(task)
    {
        return Err(Error::RevisionConflict);
    }
    let hash = execution_target::accepted_fingerprint(&command, &target, reply_binding.as_ref())?;
    let intent = command.into_accepted_intent(&dialogue, reply_binding)?;
    let epoch = integer(intent.accepted_cursor.epoch)?;
    let seq = integer(intent.accepted_cursor.seq)?;
    let bytes = content_bytes(&intent.content_ref);
    let inputs = musteragent_persistence_codec::dialogue_codec::input_attachments_bytes(
        &intent.input_attachments,
    );
    let public: Vec<_> = intent
        .input_attachments
        .iter()
        .map(|value| value.attachment.clone())
        .collect();
    let public_bytes = musteragent_persistence_codec::dialogue_codec::attachments_bytes(&public);
    if inputs.len() > 32 * 1024 || public_bytes.len() > 32 * 1024 {
        return Err(Error::Unavailable);
    }
    let count = i16::try_from(public.len()).map_err(|_| Error::Unavailable)?;
    // Lock every ready ContentRef in the original global order before FK
    // inserts; GC and acceptance serialize on the same Content Owner rows.
    let mut refs = vec![(
        "user_intent",
        intent.intent_id.into_uuid(),
        &intent.content_ref,
    )];
    refs.extend(public.iter().map(|value| {
        (
            "user_intent",
            intent.intent_id.into_uuid(),
            &value.content_ref,
        )
    }));
    if let Some(binding) = &intent.reply_binding {
        refs.extend(
            binding
                .content_refs()
                .into_iter()
                .map(|reference| ("user_intent", intent.intent_id.into_uuid(), reference)),
        );
    }
    retain_content_refs(tx, actor, dialogue.agent_id, refs).await?;
    sqlx::query(
            "INSERT INTO agent.dialogue_user_intents(intent_id,tenant_id,owner_id,dialogue_id,command_id,source_entry_id,content_ref,origin_surface,origin_installation_id,observed_epoch,observed_seq,reply_to_intent_id,accepted_epoch,accepted_seq,accepted_at_ms,input_attachments,input_attachment_count,task_target,reply_binding) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
        )
            .bind(intent.intent_id.into_uuid())
.bind(actor.tenant_id.into_uuid())
.bind(actor.principal_id.into_uuid())
.bind(dialogue.dialogue_id.into_uuid())
.bind(intent.command_id.into_uuid())
.bind(intent.source_entry_id.into_uuid())
.bind(&bytes)
.bind(intent.origin.surface.as_str())
.bind(intent.origin.installation_id.map(DeviceInstallationId::into_uuid))
.bind(integer(intent.observed_cursor.epoch)?)
.bind(integer(intent.observed_cursor.seq)?)
.bind(intent.reply_to_intent_id.map(UserIntentId::into_uuid))
.bind(epoch)
.bind(seq)
.bind(intent.accepted_at.unix_millis())
.bind(inputs)
.bind(count)
.bind(task_target::encode(intent.task_target))
.bind(crate::dialogue_codec::reply_binding_bytes(intent.reply_binding.as_ref()))
.execute(&mut *tx).await.map_err(db)?;
    attachments::save(tx, &intent).await?;
    execution_target::stage(tx, &intent, &target).await?;
    sqlx::query(
            "INSERT INTO agent.agent_dialogue_entries(entry_id,tenant_id,owner_id,dialogue_id,epoch,seq,kind,source_intent_id,content_ref,visible_revision,created_at_ms,input_attachments,input_attachment_count) VALUES($1,$2,$3,$4,$5,$6,'user_message',$7,$8,1,$9,$10,$11)",
        )
            .bind(intent.source_entry_id.into_uuid())
.bind(actor.tenant_id.into_uuid())
.bind(actor.principal_id.into_uuid())
.bind(dialogue.dialogue_id.into_uuid())
.bind(epoch)
.bind(seq)
.bind(intent.intent_id.into_uuid())
.bind(bytes)
.bind(intent.accepted_at.unix_millis())
.bind(public_bytes)
.bind(count)
.execute(&mut *tx).await.map_err(db)?;
    sqlx::query(
        "UPDATE agent.agent_dialogues
                 SET head_seq=$2
                 WHERE dialogue_id=$1",
    )
    .bind(dialogue.dialogue_id.into_uuid())
    .bind(seq)
    .execute(&mut *tx)
    .await
    .map_err(db)?;
    let value = crate::dialogue_codec::intent(&intent);
    save_receipt(
        tx,
        actor,
        intent.command_id,
        "accept_intent",
        hash,
        wire::OwnerCommandReceipt {
            result: Some(wire::owner_command_receipt::Result::AcceptedIntent(
                wire::AcceptedDialogueUserIntent {
                    intent: Some(value.clone()),
                    replayed: false,
                },
            )),
        },
        intent.accepted_at,
    )
    .await?;
    outbox(
        tx,
        actor,
        crate::dialogue_codec::AccountOutboxEvent {
            topic: "agent_dialogue",
            aggregate: dialogue.dialogue_id.into_uuid(),
            cursor: intent.accepted_cursor,
            event: "user_intent_accepted",
            payload: wire::AccountChange {
                event: Some(wire::account_change::Event::UserIntentAccepted(value)),
            },
            at: intent.accepted_at,
        },
    )
    .await?;
    crate::dialogue_handoff::progress::refresh(tx, actor, intent.intent_id, None, false).await?;
    Ok(AcceptedDialogueIntent {
        intent,
        replayed: false,
    })
}
impl AgentDialogueStore for PostgresAgentDialogueStore {
    fn read_intent_receipt(
        &self,
        actor: Actor,
        dialogue_id: AgentDialogueId,
        command_id: CommandId,
    ) -> ApplicationFuture<'_, Result<DialogueIntentReceiptObservation, Error>> {
        Box::pin(receipt::read(
            &self.database,
            actor,
            dialogue_id,
            command_id,
        ))
    }
    fn read_history(
        &self,
        actor: Actor,
        dialogue_id: AgentDialogueId,
        query: DialogueHistoryQuery,
    ) -> ApplicationFuture<'_, Result<DialogueHistoryPage, Error>> {
        Box::pin(self.history_inner(actor, dialogue_id, query))
    }
    fn accept(
        &self,
        command: AcceptDialogueIntent,
    ) -> ApplicationFuture<'_, Result<AcceptedDialogueIntent, Error>> {
        Box::pin(self.accept_inner(command, DialogueIntentExecutionTarget::Cloud))
    }
    fn accept_on_target(
        &self,
        command: AcceptDialogueIntent,
        target: DialogueIntentExecutionTarget,
    ) -> ApplicationFuture<'_, Result<AcceptedDialogueIntent, Error>> {
        Box::pin(self.accept_inner(command, target))
    }
    fn load(
        &self,
        actor: Actor,
        dialogue_id: AgentDialogueId,
    ) -> ApplicationFuture<'_, Result<Option<AgentDialogue>, Error>> {
        Box::pin(async move {
            require_user(actor)?;
            sqlx::query(
                "SELECT d.*
                 FROM agent.agent_dialogues d
                 JOIN agent.owned_agents a USING(agent_id)
                 WHERE d.dialogue_id=$1
                 AND d.tenant_id=$2
                 AND d.owner_id=$3
                 AND a.status<>'deleted'",
            )
            .bind(dialogue_id.into_uuid())
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .as_ref()
            .map(dialogue_row)
            .transpose()
        })
    }
    fn read_page(
        &self,
        actor: Actor,
        dialogue_id: AgentDialogueId,
        after: DialogueCursor,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<AgentDialoguePage, Error>> {
        Box::pin(async move {
            require_user(actor)?;
            let mut tx = self
                .database
                .read_unit_of_work(
                    musteragent_telemetry::DbCommitKind::Projection,
                    crate::PostgresTransactionIsolation::RepeatableRead,
                )
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            let row = sqlx::query(
                "SELECT d.*
                 FROM agent.agent_dialogues d
                 JOIN agent.owned_agents a USING(agent_id)
                 WHERE d.dialogue_id=$1
                 AND d.tenant_id=$2
                 AND d.owner_id=$3
                 AND a.status<>'deleted'",
            )
            .bind(dialogue_id.into_uuid())
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(Error::Unavailable)?;
            let dialogue = dialogue_row(&row)?;
            after
                .require_at_or_before(dialogue.head)
                .map_err(|_| Error::RevisionConflict)?;
            let limit = usize::from(limit.clamp(1, 200));
            let rows = sqlx::query(
                "SELECT *
                 FROM (SELECT e.*, i.command_id AS client_command_id FROM agent.agent_dialogue_entries e LEFT JOIN agent.dialogue_user_intents i ON i.intent_id=e.source_intent_id AND e.kind='user_message' AND e.author_agent_id IS NULL) AS history_entries
                 WHERE dialogue_id=$1
                 AND tenant_id=$2
                 AND owner_id=$3
                 AND epoch=$4
                 AND seq>$5
                 AND seq<=$6
                 ORDER BY seq LIMIT $7",
            )
            .bind(dialogue_id.into_uuid())
            .bind(actor.tenant_id.into_uuid())
            .bind(actor.principal_id.into_uuid())
            .bind(integer(after.epoch)?)
            .bind(integer(after.seq)?)
            .bind(integer(dialogue.head.seq)?)
            .bind((limit + 1) as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
            let has_more = rows.len() > limit;
            let mut entries = rows
                .iter()
                .take(limit)
                .map(history_entry_row)
                .collect::<Result<Vec<_>, _>>()?;
            progress::attach(&mut tx, actor, entries.iter_mut()).await?;
            let activity = crate::dialogue_execution_activity::read(
                &mut tx,
                dialogue.agent_id,
                dialogue.dialogue_id,
            )
            .await?;
            reactions::attach(&mut tx, actor, entries.iter_mut()).await?;
            crate::agent_work_creations::attach(&mut tx, actor, entries.iter_mut()).await?;
            crate::dialogue_publication::reply::attach(&mut tx, actor, entries.iter_mut()).await?;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            Ok(AgentDialoguePage {
                activity,
                dialogue,
                entries,
                has_more,
            })
        })
    }
}
pub(super) fn entry_row(row: &PgRow) -> Result<AgentDialogueEntry, Error> {
    let content = match row.try_get::<&str, _>("kind").map_err(db)? {
        "erased" => AgentDialogueContent::Erased {
            original_source_payload_sha256: musteragent_content_ref::Sha256::from_bytes(
                row.try_get::<Vec<u8>, _>("erased_source_payload_sha256")
                    .map_err(db)?,
            )
            .map_err(|_| Error::Corrupt)?,
        },
        "partner_created" => AgentDialogueContent::PartnerCreated(AgentDialoguePartnerCreated {
            creation_command_id: CommandId::from_uuid(
                row.try_get("partner_creation_command_id").map_err(db)?,
            )
            .map_err(|_| Error::Corrupt)?,
            agent_id: AgentId::from_uuid(row.try_get("partner_agent_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
            dialogue_id: AgentDialogueId::from_uuid(
                row.try_get("partner_dialogue_id").map_err(db)?,
            )
            .map_err(|_| Error::Corrupt)?,
            display_name: row.try_get("partner_display_name").map_err(db)?,
            description: row.try_get("partner_description").map_err(db)?,
            avatar_ref: row
                .try_get::<Option<Vec<u8>>, _>("partner_avatar_ref")
                .map_err(db)?
                .map(content_from_bytes)
                .transpose()?,
            profile_revision: nz(unsigned(
                row.try_get("partner_profile_revision").map_err(db)?,
            )?)?,
        }),
        "user_message" => AgentDialogueContent::UserMessage {
            content_ref: content_from_bytes(row.try_get("content_ref").map_err(db)?)?,
            input_attachments: {
                let refs = musteragent_persistence_codec::dialogue_codec::attachments_from_bytes(
                    row.try_get("input_attachments").map_err(db)?,
                )
                .map_err(|_| Error::Corrupt)?;
                if refs.len()
                    != usize::try_from(
                        row.try_get::<i16, _>("input_attachment_count")
                            .map_err(db)?,
                    )
                    .map_err(|_| Error::Corrupt)?
                {
                    return Err(Error::Corrupt);
                }
                refs
            },
        },
        "agent_message" => AgentDialogueContent::AgentMessage {
            content_ref: content_from_bytes(row.try_get("content_ref").map_err(db)?)?,
        },
        "interaction" => AgentDialogueContent::Interaction {
            interaction_id: InteractionId::from_uuid(row.try_get("interaction_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
        },
        "artifact" => AgentDialogueContent::Artifact {
            artifact_id: ArtifactId::from_uuid(row.try_get("artifact_id").map_err(db)?)
                .map_err(|_| Error::Corrupt)?,
            artifact_version_id: ArtifactVersionId::from_uuid(
                row.try_get("artifact_version_id").map_err(db)?,
            )
            .map_err(|_| Error::Corrupt)?,
        },
        _ => return Err(Error::Corrupt),
    };
    Ok(AgentDialogueEntry {
        entry_id: DialogueEntryId::from_uuid(row.try_get("entry_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        dialogue_id: AgentDialogueId::from_uuid(row.try_get("dialogue_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        owner_id: PrincipalId::from_uuid(row.try_get("owner_id").map_err(db)?)
            .map_err(|_| Error::Corrupt)?,
        cursor: DialogueCursor {
            epoch: unsigned(row.try_get("epoch").map_err(db)?)?,
            seq: unsigned(row.try_get("seq").map_err(db)?)?,
        },
        source_intent_id: row
            .try_get::<Option<uuid::Uuid>, _>("source_intent_id")
            .map_err(db)?
            .map(UserIntentId::from_uuid)
            .transpose()
            .map_err(|_| Error::Corrupt)?,
        author_agent_id: row
            .try_get::<Option<uuid::Uuid>, _>("author_agent_id")
            .map_err(db)?
            .map(AgentId::from_uuid)
            .transpose()
            .map_err(|_| Error::Corrupt)?,
        visible_revision: nz(unsigned(row.try_get("visible_revision").map_err(db)?)?)?,
        content,
        created_at: UtcTimestamp::from_unix_millis(row.try_get("created_at_ms").map_err(db)?),
        // Projections that list their own columns predate presentation.
        presentation: match row.try_get::<String, _>("presentation") {
            Ok(value) => AgentDialoguePresentation::parse(&value).ok_or(Error::Corrupt)?,
            Err(sqlx::Error::ColumnNotFound(_)) => AgentDialoguePresentation::Standard,
            Err(error) => return Err(db(error)),
        },
    })
}

impl PostgresAgentDialogueStore {
    async fn history_inner(
        &self,
        actor: Actor,
        dialogue_id: AgentDialogueId,
        query: DialogueHistoryQuery,
    ) -> Result<DialogueHistoryPage, Error> {
        require_user(actor)?;
        let mut tx = self
            .database
            .read_unit_of_work(
                musteragent_telemetry::DbCommitKind::Projection,
                crate::PostgresTransactionIsolation::RepeatableRead,
            )
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        let row=sqlx::query("SELECT d.* FROM agent.agent_dialogues d JOIN agent.owned_agents a USING(agent_id) WHERE d.dialogue_id=$1 AND d.tenant_id=$2 AND d.owner_id=$3 AND a.status<>'deleted'")
            .bind(dialogue_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
        let dialogue = dialogue_row(&row)?;
        let mut entries = match query {
            DialogueHistoryQuery::Before { before, limit } => {
                if let Some(cursor) = before {
                    cursor
                        .require_at_or_before(dialogue.head)
                        .map_err(|_| Error::RevisionConflict)?;
                }
                let rows=sqlx::query("SELECT * FROM (SELECT e.*, i.command_id AS client_command_id FROM agent.agent_dialogue_entries e LEFT JOIN agent.dialogue_user_intents i ON i.intent_id=e.source_intent_id AND e.kind='user_message' AND e.author_agent_id IS NULL) AS history_entries WHERE dialogue_id=$1 AND epoch=$2 AND seq<=$3 AND ($4::bigint IS NULL OR seq<$4) ORDER BY seq DESC LIMIT $5")
                    .bind(dialogue_id.into_uuid()).bind(integer(dialogue.head.epoch)?).bind(integer(dialogue.head.seq)?).bind(before.map(|v|integer(v.seq)).transpose()?).bind(i64::from(limit.clamp(1,200))).fetch_all(&mut *tx).await.map_err(db)?;
                rows.iter()
                    .map(history_entry_row)
                    .collect::<Result<Vec<_>, _>>()?
            }
            DialogueHistoryQuery::Around { entry_id, radius } => {
                let anchor:i64=sqlx::query_scalar("SELECT seq FROM agent.agent_dialogue_entries WHERE dialogue_id=$1 AND epoch=$2 AND entry_id=$3 AND seq<=$4")
                    .bind(dialogue_id.into_uuid()).bind(integer(dialogue.head.epoch)?).bind(entry_id.into_uuid()).bind(integer(dialogue.head.seq)?).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(Error::Unavailable)?;
                let radius = i64::from(radius.min(50));
                let rows=sqlx::query("(SELECT * FROM (SELECT e.*, i.command_id AS client_command_id FROM agent.agent_dialogue_entries e LEFT JOIN agent.dialogue_user_intents i ON i.intent_id=e.source_intent_id AND e.kind='user_message' AND e.author_agent_id IS NULL) AS history_entries WHERE dialogue_id=$1 AND epoch=$2 AND seq<=$3 ORDER BY seq DESC LIMIT $5) UNION ALL (SELECT * FROM (SELECT e.*, i.command_id AS client_command_id FROM agent.agent_dialogue_entries e LEFT JOIN agent.dialogue_user_intents i ON i.intent_id=e.source_intent_id AND e.kind='user_message' AND e.author_agent_id IS NULL) AS history_entries WHERE dialogue_id=$1 AND epoch=$2 AND seq>$3 AND seq<=$4 ORDER BY seq LIMIT $6)")
                    .bind(dialogue_id.into_uuid()).bind(integer(dialogue.head.epoch)?).bind(anchor).bind(integer(dialogue.head.seq)?).bind(radius+1).bind(radius).fetch_all(&mut *tx).await.map_err(db)?;
                rows.iter()
                    .map(history_entry_row)
                    .collect::<Result<Vec<_>, _>>()?
            }
        };
        entries.sort_by_key(|e| e.entry.cursor.seq);
        let (has_before, has_after) = if let (Some(first), Some(last)) =
            (entries.first(), entries.last())
        {
            let row=sqlx::query("SELECT EXISTS(SELECT 1 FROM agent.agent_dialogue_entries WHERE dialogue_id=$1 AND epoch=$2 AND seq<$3) AS has_before, EXISTS(SELECT 1 FROM agent.agent_dialogue_entries WHERE dialogue_id=$1 AND epoch=$2 AND seq>$4 AND seq<=$5) AS has_after")
                .bind(dialogue_id.into_uuid()).bind(integer(dialogue.head.epoch)?).bind(integer(first.entry.cursor.seq)?).bind(integer(last.entry.cursor.seq)?).bind(integer(dialogue.head.seq)?).fetch_one(&mut *tx).await.map_err(db)?;
            (
                row.try_get("has_before").map_err(db)?,
                row.try_get("has_after").map_err(db)?,
            )
        } else {
            (false, dialogue.head.seq > 0)
        };
        progress::attach(&mut tx, actor, entries.iter_mut()).await?;
        let activity = crate::dialogue_execution_activity::read(
            &mut tx,
            dialogue.agent_id,
            dialogue.dialogue_id,
        )
        .await?;
        reactions::attach(&mut tx, actor, entries.iter_mut()).await?;
        crate::agent_work_creations::attach(&mut tx, actor, entries.iter_mut()).await?;
        crate::dialogue_publication::reply::attach(&mut tx, actor, entries.iter_mut()).await?;
        tx.commit()
            .await
            .map_err(crate::dialogue_codec::transaction)?;
        Ok(DialogueHistoryPage {
            activity,
            dialogue,
            entries,
            has_before,
            has_after,
        })
    }
}

fn history_entry_row(row: &PgRow) -> Result<DialogueHistoryEntry, Error> {
    Ok(DialogueHistoryEntry {
        entry: entry_row(row)?,
        admission_progress: None,
        reactions: None,
        work_creations: Vec::new(),
        reply_target: None,
        reply_context: None,
        client_command_id: row
            .try_get::<Option<uuid::Uuid>, _>("client_command_id")
            .map_err(db)?
            .map(CommandId::from_uuid)
            .transpose()
            .map_err(|_| Error::Corrupt)?,
    })
}
