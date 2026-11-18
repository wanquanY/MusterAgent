use crate::dialogue_codec::{db, decode_activation, integer, require_user, required, unsigned};
use musteragent_identity::*;
use musteragent_persistence_codec::{dialogue_codec as codec, wire};
use musteragent_runtime_contract::ApplicationFuture;
use musteragent_runtime_session::*;
use prost::Message;
use sqlx::Row;
#[derive(Clone)]
pub struct PostgresAccountStore {
    database: crate::PostgresDatabaseRuntime,
}
impl PostgresAccountStore {
    #[must_use]
    pub fn new(database: impl Into<crate::PostgresDatabaseRuntime>) -> Self {
        let database = database.into();
        Self { database }
    }
}
impl AccountStore for PostgresAccountStore {
    fn read_changes(
        &self,
        actor: Actor,
        cursor: AccountCursor,
        limit: u16,
    ) -> ApplicationFuture<'_, Result<AccountPage, AccountError>> {
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
            let row=sqlx::query("SELECT sync_epoch,head_event_seq FROM agent.owner_spaces WHERE space_id=$1 AND tenant_id=$2 AND owner_id=$3 AND product='musteragent'")
        .bind(cursor.space_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
            let head = AccountCursor {
                space_id: cursor.space_id,
                epoch: unsigned(row.try_get("sync_epoch").map_err(db)?)?,
                seq: unsigned(row.try_get("head_event_seq").map_err(db)?)?,
            };
            if cursor.epoch != head.epoch || cursor.seq > head.seq {
                return Err(AccountError::CursorMismatch { head });
            }
            let limit = usize::from(limit.clamp(1, 100));
            let rows=sqlx::query("SELECT account_seq,event_kind,created_at_ms,CASE WHEN octet_length(payload)<=65536 THEN payload END AS payload FROM agent.owner_outbox WHERE space_id=$1 AND tenant_id=$2 AND owner_id=$3 AND account_epoch=$4 AND account_seq>$5 AND account_seq<=$6 ORDER BY account_seq LIMIT $7")
        .bind(cursor.space_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).bind(integer(cursor.epoch)?).bind(integer(cursor.seq)?).bind(integer(head.seq)?).bind(limit as i64).fetch_all(&mut *tx).await.map_err(db)?;
            let mut changes = Vec::with_capacity(rows.len());
            let mut expected = cursor.seq;
            for row in rows {
                expected = expected.checked_add(1).ok_or(DialogueStoreError::Corrupt)?;
                let seq = unsigned(row.try_get("account_seq").map_err(db)?)?;
                if seq != expected {
                    return Err(AccountError::CursorMismatch { head });
                }
                let payload: Vec<u8> =
                    required(row.try_get::<Option<Vec<u8>>, _>("payload").map_err(db)?)?;
                let wire = wire::AccountChange::decode(payload.as_slice())
                    .map_err(|_| DialogueStoreError::Corrupt)?;
                let kind = row.try_get::<&str, _>("event_kind").map_err(db)?;
                let change = match (kind, wire.event) {
                    ("work_changed", Some(wire::account_change::Event::WorkChanged(value))) => {
                        let agent_id: AgentId = value
                            .agent_id
                            .ok_or(DialogueStoreError::Corrupt)?
                            .value
                            .parse()
                            .map_err(|_| DialogueStoreError::Corrupt)?;
                        let reference = value
                            .reference
                            .map(|value| {
                                let subject_agent: AgentId = value
                                    .agent_id
                                    .ok_or(DialogueStoreError::Corrupt)?
                                    .value
                                    .parse()
                                    .map_err(|_| DialogueStoreError::Corrupt)?;
                                let id = value
                                    .id
                                    .parse::<uuid::Uuid>()
                                    .map_err(|_| DialogueStoreError::Corrupt)?;
                                let kind = match value.kind {
                                    1 => AgentWorkKind::WorkItem,
                                    2 => AgentWorkKind::Schedule,
                                    3 => AgentWorkKind::Goal,
                                    _ => return Err(DialogueStoreError::Corrupt),
                                };
                                if id.is_nil() || subject_agent != agent_id {
                                    return Err(DialogueStoreError::Corrupt);
                                }
                                Ok(AgentWorkReference { kind, id, agent_id })
                            })
                            .transpose()?;
                        let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id) WHERE a.agent_id=$1 AND s.space_id=$2 AND s.tenant_id=$3 AND s.owner_id=$4)")
                            .bind(agent_id.into_uuid()).bind(cursor.space_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
                            .fetch_one(&mut *tx).await.map_err(db)?;
                        if !valid {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::WorkChanged {
                            agent_id,
                            reference,
                        }
                    }
                    (
                        "event_subscription_changed",
                        Some(wire::account_change::Event::EventSubscriptionChanged(value)),
                    ) => {
                        let source = musteragent_persistence_codec::owner_event_sources_codec::decode_subscription(Some(value))
                            .map_err(|_| DialogueStoreError::Corrupt)?;
                        if source.subject.tenant_id != actor.tenant_id
                            || source.subject.owner_id != actor.principal_id
                        {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::EventSubscriptionChanged(source)
                    }

                    (
                        "activity_changed",
                        Some(wire::account_change::Event::ActivityChanged(value)),
                    ) => {
                        let activity =
                            musteragent_persistence_codec::dialogue_execution_activity_codec::decode(
                                value.activity.ok_or(DialogueStoreError::Corrupt)?,
                            )
                            .map_err(|_| DialogueStoreError::Corrupt)?;
                        let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.owned_agents a JOIN agent.owner_spaces s USING(space_id) WHERE a.agent_id=$1 AND a.dialogue_id=$2 AND s.space_id=$3 AND s.tenant_id=$4 AND s.owner_id=$5)")
                            .bind(activity.agent_id.into_uuid()).bind(activity.dialogue_id.into_uuid()).bind(cursor.space_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
                        if !valid {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::ActivityChanged(activity)
                    }

                    (
                        "file_head_changed",
                        Some(wire::account_change::Event::FileHeadChanged(v)),
                    ) => {
                        let file = AgentFileHeadChanged {
                            agent_id: codec::id(v.agent_id.map(|v| v.value))
                                .map_err(|_| DialogueStoreError::Corrupt)?,
                            dialogue_id: codec::id(v.dialogue_id.map(|v| v.value))
                                .map_err(|_| DialogueStoreError::Corrupt)?,
                            entry_id: codec::id(v.entry_id.map(|v| v.value))
                                .map_err(|_| DialogueStoreError::Corrupt)?,
                            artifact_id: codec::id(v.artifact_id.map(|v| v.value))
                                .map_err(|_| DialogueStoreError::Corrupt)?,
                            current_version_id: codec::id(v.current_version_id.map(|v| v.value))
                                .map_err(|_| DialogueStoreError::Corrupt)?,
                            version: VersionNumber::new(v.version)
                                .map_err(|_| DialogueStoreError::Corrupt)?,
                        };
                        // Historical cursor replay is valid after subsequent edits or erasure.
                        // Check immutable account/file placement, not the mutable current head.
                        let valid=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM agent.owned_agents a
                            JOIN agent.owner_spaces s ON s.space_id=a.space_id
                            JOIN agent.agent_dialogue_entries e ON e.dialogue_id=a.dialogue_id AND e.tenant_id=s.tenant_id AND e.owner_id=s.owner_id
                            JOIN artifact.artifact_versions v ON v.artifact_id=e.artifact_id
                            WHERE s.space_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 AND a.agent_id=$4 AND a.dialogue_id=$5
                            AND e.entry_id=$6 AND e.artifact_id=$7 AND v.artifact_version_id=$8 AND v.version=$9::numeric)")
                            .bind(cursor.space_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
                            .bind(file.agent_id.into_uuid()).bind(file.dialogue_id.into_uuid()).bind(file.entry_id.into_uuid())
                            .bind(file.artifact_id.into_uuid()).bind(file.current_version_id.into_uuid()).bind(file.version.get().to_string())
                            .fetch_one(&mut *tx).await.map_err(db)?;
                        if !valid {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::FileHeadChanged(file)
                    }

                    ("agent_activated", Some(wire::account_change::Event::AgentActivated(v))) => {
                        let value = decode_activation(v)?;
                        if value.space.space_id != cursor.space_id
                            || value.space.tenant_id != actor.tenant_id
                            || value.space.owner_id != actor.principal_id
                        {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::AgentActivated(value)
                    }
                    ("agent_deleted", Some(wire::account_change::Event::AgentDeleted(v))) => {
                        let value =
                            codec::decode_agent(v).map_err(|_| DialogueStoreError::Corrupt)?;
                        if value.space_id != cursor.space_id
                            || value.status
                                != musteragent_definition::owned_agent::OwnedAgentStatus::Deleted
                        {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::AgentDeleted(value)
                    }
                    (
                        "agent_version_changed",
                        Some(wire::account_change::Event::AgentVersionChanged(v)),
                    ) => {
                        let value =
                            codec::decode_agent(v).map_err(|_| DialogueStoreError::Corrupt)?;
                        if value.space_id != cursor.space_id {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::AgentVersionChanged(value)
                    }
                    (
                        "user_intent_accepted",
                        Some(wire::account_change::Event::UserIntentAccepted(v)),
                    ) => {
                        let value =
                            codec::decode_intent(v).map_err(|_| DialogueStoreError::Corrupt)?;
                        if value.tenant_id != actor.tenant_id
                            || value.owner_id != actor.principal_id
                        {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::UserIntentAccepted(value)
                    }
                    (
                        "configuration_changed",
                        Some(wire::account_change::Event::ConfigurationChanged(v)),
                    ) => AccountChange::ConfigurationChanged(
                        codec::decode_configuration(v).map_err(|_| DialogueStoreError::Corrupt)?,
                    ),
                    (
                        "public_entry_published",
                        Some(wire::account_change::Event::PublicEntryPublished(v)),
                    ) => {
                        let entry =
                            codec::decode_entry(v).map_err(|_| DialogueStoreError::Corrupt)?;
                        if entry.tenant_id != actor.tenant_id
                            || entry.owner_id != actor.principal_id
                            || entry.author_agent_id.is_none()
                            || matches!(
                                entry.content,
                                musteragent_execution_context::AgentDialogueContent::UserMessage { .. }
                            )
                        {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::PublicEntryPublished(entry)
                    }
                    (
                        "public_entry_changed",
                        Some(wire::account_change::Event::PublicEntryChanged(v)),
                    ) => {
                        let entry =
                            codec::decode_entry(v).map_err(|_| DialogueStoreError::Corrupt)?;
                        if entry.tenant_id != actor.tenant_id
                            || entry.owner_id != actor.principal_id
                            || entry.author_agent_id.is_none()
                            || matches!(
                                entry.content,
                                musteragent_execution_context::AgentDialogueContent::UserMessage { .. }
                            )
                        {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::PublicEntryChanged(entry)
                    }
                    (
                        "reactions_changed",
                        Some(wire::account_change::Event::ReactionsChanged(v)),
                    ) => {
                        let value =
                            musteragent_persistence_codec::public_reactions_codec::decode_changed(
                                v,
                            )
                            .map_err(|_| DialogueStoreError::Corrupt)?;
                        // This is only an invalidation pointer. The scoped current read
                        // rechecks deletion/erasure before disclosing a feedback set.
                        let owned:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.agent_dialogue_entries e JOIN agent.agent_dialogues d USING(dialogue_id,tenant_id,owner_id) WHERE e.entry_id=$1 AND e.dialogue_id=$2 AND d.agent_id=$3 AND e.tenant_id=$4 AND e.owner_id=$5)")
                            .bind(value.entry_id.into_uuid()).bind(value.dialogue_id.into_uuid()).bind(value.agent_id.into_uuid())
                            .bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
                        if !owned {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::ReactionsChanged(value)
                    }
                    (
                        "admission_progress_changed",
                        Some(wire::account_change::Event::AdmissionProgressChanged(v)),
                    ) => {
                        let value =
                            musteragent_persistence_codec::dialogue_admission_progress_codec::decode(v)
                                .map_err(|_| DialogueStoreError::Corrupt)?;
                        // Account outbox ownership was checked above; prove the original relation too.
                        let matches:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent.dialogue_user_intents i JOIN agent.agent_dialogues d ON d.dialogue_id=i.dialogue_id WHERE i.intent_id=$1 AND i.tenant_id=$2 AND i.owner_id=$3 AND i.source_entry_id=$4 AND i.dialogue_id=$5 AND d.agent_id=$6)")
                            .bind(value.intent_id.into_uuid()).bind(actor.tenant_id.into_uuid()).bind(actor.principal_id.into_uuid())
                            .bind(value.entry_id.into_uuid()).bind(value.dialogue_id.into_uuid()).bind(value.agent_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
                        if !matches {
                            return Err(DialogueStoreError::Corrupt.into());
                        }
                        AccountChange::AdmissionProgressChanged(value)
                    }
                    _ => return Err(DialogueStoreError::Corrupt.into()),
                };
                changes.push(AccountEvent {
                    cursor: AccountCursor { seq, ..cursor },
                    change,
                    created_at: UtcTimestamp::from_unix_millis(
                        row.try_get("created_at_ms").map_err(db)?,
                    ),
                });
            }
            if changes.len() < limit && expected != head.seq {
                return Err(AccountError::CursorMismatch { head });
            }
            let has_more = expected < head.seq;
            tx.commit()
                .await
                .map_err(crate::dialogue_codec::transaction)?;
            Ok(AccountPage {
                head,
                changes,
                has_more,
            })
        })
    }
}
