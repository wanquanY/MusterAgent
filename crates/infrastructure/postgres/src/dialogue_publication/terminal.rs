//! Durable resolution of a Dialogue Run's terminal public-delivery obligation.
//! This is a capability of the existing Publication Owner, not a new publisher.
use super::*;
use musteragent_content_ref::{ContentRef, MediaType, Sha256};
use musteragent_projection::LedgerPayloadDecoder;
use musteragent_runtime_content::{ContentStore, PutContent};
use std::sync::Arc;
mod candidates;
mod proof;
mod worker;
pub(crate) use proof::{TerminalRunPosition, register_terminal_in_transaction};
type Outcome = PublicTerminalDeliveryOutcome;

#[derive(Clone)]
pub struct PostgresPublicTerminalDeliveryStore {
    pool: sqlx::PgPool,
    database: crate::PostgresDatabaseRuntime,
    content: Arc<dyn ContentStore>,
    decoder: Arc<dyn LedgerPayloadDecoder>,
}
impl PostgresPublicTerminalDeliveryStore {
    pub fn new(
        database: impl Into<crate::PostgresDatabaseRuntime>,
        content: Arc<dyn ContentStore>,
        decoder: Arc<dyn LedgerPayloadDecoder>,
    ) -> Self {
        let database = database.into();
        Self {
            pool: database.pool().clone(),
            database,
            content,
            decoder,
        }
    }

    /// Internal worker entry. Scope and content are never selected by a caller.
    /// Replays consume the original obligation; no Attempt/lease is fabricated.
    pub async fn publish_terminal(
        &self,
        tenant: TenantId,
        run: RunId,
    ) -> Result<Outcome, DialoguePublicationError> {
        // Prepare only deterministic Owner text outside the ledger commit lane.
        let initial = proof::load(
            &mut *self.pool.acquire().await.map_err(db)?,
            tenant,
            run,
            self.decoder.as_ref(),
        )
        .await?;
        let Some(initial) = initial else {
            return Ok(Outcome::AlreadyResolved);
        };
        let notice = initial.notice;
        let body = self
            .content
            .put(PutContent {
                tenant_id: tenant,
                media_type: MediaType::new("text/markdown;charset=utf-8")
                    .map_err(|_| DialogueStoreError::Corrupt)?,
                bytes: notice.message().as_bytes().to_vec(),
            })
            .await
            .map_err(|_| DialogueStoreError::Infrastructure)?;
        let mut tx = self
            .database
            .write_unit_of_work(DbCommitKind::Resource)
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Acquire, error))?;
        // Same lock order as original execution/publication commits.
        sqlx::query("SELECT conversation_id FROM event_store.ledger_streams WHERE tenant_id=$1 AND conversation_id=$2 FOR UPDATE")
            .bind(tenant.into_uuid()).bind(initial.conversation_id.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        sqlx::query("SELECT run_id FROM runtime.runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE")
            .bind(tenant.into_uuid())
            .bind(run.into_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("SELECT run_id FROM agent.dialogue_terminal_deliveries WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE")
            .bind(tenant.into_uuid()).bind(run.into_uuid()).fetch_one(&mut *tx).await.map_err(db)?;
        let Some(proof) = proof::load(&mut tx, tenant, run, self.decoder.as_ref()).await? else {
            tx.commit()
                .await
                .map_err(|error| transaction_error(DbTransactionPhase::Commit, error))?;
            return Ok(Outcome::AlreadyResolved);
        };
        if proof.notice != notice || proof.record_id != initial.record_id {
            return Err(DialogueStoreError::Corrupt.into());
        }
        if let Some(reason) = proof.suppression {
            if proof.requires_effect_reconciliation {
                // Cancellation or another Run transition cannot erase an
                // external request already dispatched by its original Owner.
                return Err(DialogueStoreError::Unavailable.into());
            }
            resolve(&mut tx, tenant, run, reason, None).await?;
            tx.commit()
                .await
                .map_err(|error| transaction_error(DbTransactionPhase::Commit, error))?;
            return Ok(Outcome::Suppressed);
        }
        let mut source = load_source(&mut tx, run).await?;
        if !lock_current_scope(&mut tx, &mut source).await? {
            if proof.requires_effect_reconciliation {
                // Inactive cannot erase an old external effect. Retain the
                // durable obligation until the original Effect/Interaction
                // Owner resolves or exposes its currently authorized actions.
                return Err(DialogueStoreError::Inactive.into());
            }
            resolve(&mut tx, tenant, run, "agent_inactive", None).await?;
            tx.commit()
                .await
                .map_err(|error| transaction_error(DbTransactionPhase::Commit, error))?;
            return Ok(Outcome::Suppressed);
        }
        // Only the last complete, explicitly qualified output can cover this
        // request. Old cards or updates never suppress a useful blocked answer.
        if proof.notice != DialogueTerminalNotice::OutcomeUnconfirmed
            && let Some(entry) = candidates::recover(&mut tx, &source, run, &proof).await?
        {
            let (reason, entry) = match entry {
                candidates::Recovery::PreviouslyPublished => ("already_delivered", None),
                candidates::Recovery::Published(id) => ("qualified_delivery", Some(id)),
            };
            resolve(&mut tx, tenant, run, reason, entry).await?;
            tx.commit()
                .await
                .map_err(|error| transaction_error(DbTransactionPhase::Commit, error))?;
            return Ok(if entry.is_some() {
                Outcome::Delivered
            } else {
                Outcome::Suppressed
            });
        }
        // This message uses current account visibility, never the expired
        // execution permission as authority to expose additional resources.
        let entry = publish_in_transaction(
            &mut tx,
            &source,
            &Delivery {
                delivery_id: proof.delivery_id,
                run_id: run,
                purpose: DialoguePublicationPurpose::Blocked,
                fingerprint: notice_fingerprint(&proof, &body),
                content: PublicDeliveryContent::Message(body),
                source: PublicationSource::TerminalOwner(proof.record_id),
            },
        )
        .await?;
        resolve(&mut tx, tenant, run, "notice", Some(entry.entry_id)).await?;
        tx.commit()
            .await
            .map_err(|error| transaction_error(DbTransactionPhase::Commit, error))?;
        Ok(Outcome::Delivered)
    }
}

pub(super) async fn lock_current_scope(
    tx: &mut sqlx::PgConnection,
    source: &mut Source,
) -> Result<bool, Error> {
    sqlx::query("SELECT s.space_id FROM agent.owner_spaces s JOIN agent.owned_agents a USING(space_id) WHERE a.agent_id=$1 AND s.tenant_id=$2 AND s.owner_id=$3 FOR UPDATE OF s")
        .bind(source.agent_id.into_uuid()).bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid())
        .fetch_optional(&mut *tx).await.map_err(db)?.ok_or(DialogueStoreError::Unavailable)?;
    let row = sqlx::query("SELECT a.status,c.permission_revision FROM agent.agent_dialogues d JOIN agent.owned_agents a USING(agent_id) JOIN agent.owned_agent_configurations c USING(agent_id) WHERE d.dialogue_id=$1 AND d.tenant_id=$2 AND d.owner_id=$3 FOR UPDATE OF d FOR SHARE OF a,c")
        .bind(source.dialogue_id.into_uuid()).bind(source.actor.tenant_id.into_uuid()).bind(source.actor.principal_id.into_uuid())
        .fetch_one(&mut *tx).await.map_err(db)?;
    source.permission_revision = unsigned(row.try_get("permission_revision").map_err(db)?)?;
    Ok(row.try_get::<&str, _>("status").map_err(db)? == "active")
}
async fn resolve(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    run: RunId,
    reason: &str,
    entry: Option<DialogueEntryId>,
) -> Result<(), Error> {
    sqlx::query("UPDATE agent.dialogue_terminal_deliveries SET state=$3,resolution=$4,entry_id=$5,resolved_at_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint WHERE tenant_id=$1 AND run_id=$2 AND state='pending'")
        .bind(tenant.into_uuid()).bind(run.into_uuid()).bind(if entry.is_some(){"delivered"}else{"suppressed"}).bind(reason).bind(entry.map(DialogueEntryId::into_uuid))
        .execute(tx).await.map_err(db)?;
    Ok(())
}
fn notice_fingerprint(proof: &proof::TerminalProof, body: &ContentRef) -> Sha256 {
    let mut bytes = b"musteragent.terminal-owner-publication.v1\0".to_vec();
    bytes.extend_from_slice(proof.record_id.into_uuid().as_bytes());
    bytes.extend_from_slice(&proof.source_sha256.into_bytes());
    bytes.extend_from_slice(&body.sha256.into_bytes());
    Sha256::digest(bytes)
}
